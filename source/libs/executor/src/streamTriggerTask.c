/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "streamTriggerTask.h"

#include "executorInt.h"
#include "filter.h"
#include "tdatablock.h"

#define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
#define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN

typedef struct SStreamWindowTriggerTask {
  SStreamTriggerTask base;

  // construct info
  SNodeList        *pGroupKeys;
  SWindowPhysiNode *pWindow;
  int32_t           primaryTsIndex;

  // runtime state
  SGroupByColumnSupporter groupSup;
  SSHashObj              *pGroupProcessors;

} SStreamWindowTriggerTask;

typedef enum EStrwtWindowType {
  STRWT_WINDOW_TYPE_INTERVAL = 0,
  STRWT_WINDOW_TYPE_SESSION,
  STRWT_WINDOW_TYPE_STATE,
  STRWT_WINDOW_TYPE_EVENT,
  STRWT_WINDOW_TYPE_COUNT,
} EStrwtWindowType;

typedef struct SStrwtGroupProcessor {
  int64_t                   groupId;
  EStrwtWindowType          windowType;
  SStreamWindowTriggerTask *pTask;

  STimeWindow curWindow;
} SStrwtGroupProcessor;

typedef struct SStrwtIntervalGroupProcessor {
  SStrwtGroupProcessor base;
  SInterval            interval;
} SStrwtIntervalGroupProcessor;

typedef struct SStrwtSessionGroupProcessor {
  SStrwtGroupProcessor base;
  int64_t              gap;
} SStrwtSessionGroupProcessor;

typedef struct SStrwtStateGroupProcessor {
  SStrwtGroupProcessor base;
  SColumn              stateCol;
  SStateKeys           stateKey;
} SStrwtStateGroupProcessor;

typedef struct SStrwtEventGroupProcessor {
  SStrwtGroupProcessor base;
  SFilterInfo         *pStartCondInfo;
  SFilterInfo         *pEndCondInfo;
} SStrwtEventGroupProcessor;

typedef struct SStrwtCountGroupProcessor {
  SStrwtGroupProcessor base;
  int32_t              windowCount;
  int32_t              windowSliding;
  int32_t              numOfRows;
} SStrwtCountGroupProcessor;

static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStreamWindowTriggerTask *pTask = pProcessor->pTask;

  switch (pProcessor->windowType) {
    case STRWT_WINDOW_TYPE_INTERVAL: {
      SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
      TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
      *pRes = (ts <= pProcessor->curWindow.ekey);
      break;
    }
    case STRWT_WINDOW_TYPE_SESSION: {
      SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
      SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
      TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
      *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
      break;
    }
    case STRWT_WINDOW_TYPE_STATE: {
      SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
      SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
      QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
      if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
        *pRes = pState->stateKey.isNull;
      } else if (pState->stateKey.isNull) {
        *pRes = false;
      } else {
        char *val = colDataGetData(pStateColInfoData, rowIndex);
        *pRes = compareVal(val, &pState->stateKey);
      }
      break;
    }
    case STRWT_WINDOW_TYPE_EVENT:
    case STRWT_WINDOW_TYPE_COUNT: {
      *pRes = true;
      break;
    }
    default: {
      ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);

static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SStreamWindowTriggerTask *pTask = pProcessor->pTask;
  SColumnInfoData          *ps = NULL, *pe = NULL;

  QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  int32_t nrows = pBlock->info.rows;
  if (nrows == 0) {
    goto _end;
  }

  if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
    // Check if data meets start and end conditions in batch
    SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
    SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
    int32_t            status = 0;

    code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
    QUERY_CHECK_CODE(code, lino, _end);

    code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
    QUERY_CHECK_CODE(code, lino, _end);
    code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
  for (int32_t i = 0; i < nrows; ++i) {
    if (IS_CUR_WIN_VALID(pProcessor)) {
      bool inWindow = false;
      code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
      QUERY_CHECK_CODE(code, lino, _end);
      if (!inWindow) {
        code = strwtgpCloseWindow(pProcessor, pBlock, i);
        QUERY_CHECK_CODE(code, lino, _end);
        code = strwtgpOpenWindow(pProcessor, pBlock, i);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      bool shouldStart = true;
      if (ps != NULL) {
        shouldStart = ((bool*)ps->pData)[i];
      }
      if (shouldStart) {
        code = strwtgpOpenWindow(pProcessor, pBlock, i);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        // ignore this data since it does not belong to any window
        continue;
      }
    }
    code = strwtgpUpdateState(pProcessor, pBlock, i);
    QUERY_CHECK_CODE(code, lino, _end);
    bool shouldEnd = false;
    if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
      SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
      shouldEnd = (pCount->windowCount == pCount->numOfRows);
    } else if (pe != NULL) {
      shouldEnd = ((bool*)pe->pData)[i];
    }
    if (shouldEnd) {
      code = strwtgpCloseWindow(pProcessor, pBlock, i);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pGroupKeys != NULL) {
    code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
  // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (nBlocks <= 0) {
    goto _end;
  }

  // todo(kjq): implement the window trigger process
  switch (pBlocks[0]->info.type) {
    case STREAM_NORMAL:
      // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first

    case STREAM_DELETE_DATA:

    default:
      ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
  // todo(kjq): support other trigger types
  switch (pTask->type) {
    case STREAM_PERIODIC_TRIGGER:
      return TSDB_CODE_OPS_NOT_SUPPORT;

    case STERAM_COMMIT_TRIGGER:
      return TSDB_CODE_OPS_NOT_SUPPORT;

    case STREAM_WINDOW_TRIGGER:
      return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);

    default:
      ST_TASK_ELOG("invalid trigger type %d", pTask->type);
      return TSDB_CODE_INVALID_PARA;
  }
}


/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "streamTriggerTask.h"

 #include "executorInt.h"
 #include "filter.h"
 #include "tdatablock.h"
 
 #define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
 #define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN
 
 typedef struct SStreamWindowTriggerTask {
   SStreamTriggerTask base;
 
   // construct info
   SNodeList        *pGroupKeys;
   SWindowPhysiNode *pWindow;
   int32_t           primaryTsIndex;
 
   // runtime state
   SGroupByColumnSupporter groupSup;
   SSHashObj              *pGroupProcessors;
 
 } SStreamWindowTriggerTask;
 
 typedef enum EStrwtWindowType {
   STRWT_WINDOW_TYPE_INTERVAL = 0,
   STRWT_WINDOW_TYPE_SESSION,
   STRWT_WINDOW_TYPE_STATE,
   STRWT_WINDOW_TYPE_EVENT,
   STRWT_WINDOW_TYPE_COUNT,
 } EStrwtWindowType;
 
 typedef struct SStrwtGroupProcessor {
   int64_t                   groupId;
   EStrwtWindowType          windowType;
   SStreamWindowTriggerTask *pTask;
 
   STimeWindow curWindow;
 } SStrwtGroupProcessor;
 
 typedef struct SStrwtIntervalGroupProcessor {
   SStrwtGroupProcessor base;
   SInterval            interval;
 } SStrwtIntervalGroupProcessor;
 
 typedef struct SStrwtSessionGroupProcessor {
   SStrwtGroupProcessor base;
   int64_t              gap;
 } SStrwtSessionGroupProcessor;
 
 typedef struct SStrwtStateGroupProcessor {
   SStrwtGroupProcessor base;
   SColumn              stateCol;
   SStateKeys           stateKey;
 } SStrwtStateGroupProcessor;
 
 typedef struct SStrwtEventGroupProcessor {
   SStrwtGroupProcessor base;
   SFilterInfo         *pStartCondInfo;
   SFilterInfo         *pEndCondInfo;
 } SStrwtEventGroupProcessor;
 
 typedef struct SStrwtCountGroupProcessor {
   SStrwtGroupProcessor base;
   int32_t              windowCount;
   int32_t              windowSliding;
   int32_t              numOfRows;
 } SStrwtCountGroupProcessor;
 
 static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
 
   switch (pProcessor->windowType) {
     case STRWT_WINDOW_TYPE_INTERVAL: {
       SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts <= pProcessor->curWindow.ekey);
       break;
     }
     case STRWT_WINDOW_TYPE_SESSION: {
       SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
       SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
       break;
     }
     case STRWT_WINDOW_TYPE_STATE: {
       SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
       SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
       QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
       if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
         *pRes = pState->stateKey.isNull;
       } else if (pState->stateKey.isNull) {
         *pRes = false;
       } else {
         char *val = colDataGetData(pStateColInfoData, rowIndex);
         *pRes = compareVal(val, &pState->stateKey);
       }
       break;
     }
     case STRWT_WINDOW_TYPE_EVENT:
     case STRWT_WINDOW_TYPE_COUNT: {
       *pRes = true;
       break;
     }
     default: {
       ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 
 static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
   SColumnInfoData          *ps = NULL, *pe = NULL;
 
   QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   int32_t nrows = pBlock->info.rows;
   if (nrows == 0) {
     goto _end;
   }
 
   if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
     // Check if data meets start and end conditions in batch
     SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
     SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
     int32_t            status = 0;
 
     code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
 
     code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
   for (int32_t i = 0; i < nrows; ++i) {
     if (IS_CUR_WIN_VALID(pProcessor)) {
       bool inWindow = false;
       code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
       QUERY_CHECK_CODE(code, lino, _end);
       if (!inWindow) {
         code = strwtgpCloseWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       }
     } else {
       bool shouldStart = true;
       if (ps != NULL) {
         shouldStart = ((bool*)ps->pData)[i];
       }
       if (shouldStart) {
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       } else {
         // ignore this data since it does not belong to any window
         continue;
       }
     }
     code = strwtgpUpdateState(pProcessor, pBlock, i);
     QUERY_CHECK_CODE(code, lino, _end);
     bool shouldEnd = false;
     if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
       SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
       shouldEnd = (pCount->windowCount == pCount->numOfRows);
     } else if (pe != NULL) {
       shouldEnd = ((bool*)pe->pData)[i];
     }
     if (shouldEnd) {
       code = strwtgpCloseWindow(pProcessor, pBlock, i);
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   if (pGroupKeys != NULL) {
     code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
   QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
   // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   if (nBlocks <= 0) {
     goto _end;
   }
 
   // todo(kjq): implement the window trigger process
   switch (pBlocks[0]->info.type) {
     case STREAM_NORMAL:
       // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first
 
     case STREAM_DELETE_DATA:
 
     default:
       ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   // todo(kjq): support other trigger types
   switch (pTask->type) {
     case STREAM_PERIODIC_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STERAM_COMMIT_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STREAM_WINDOW_TRIGGER:
       return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);
 
     default:
       ST_TASK_ELOG("invalid trigger type %d", pTask->type);
       return TSDB_CODE_INVALID_PARA;
   }
 }
 
 
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "streamTriggerTask.h"

 #include "executorInt.h"
 #include "filter.h"
 #include "tdatablock.h"
 
 #define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
 #define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN
 
 typedef struct SStreamWindowTriggerTask {
   SStreamTriggerTask base;
 
   // construct info
   SNodeList        *pGroupKeys;
   SWindowPhysiNode *pWindow;
   int32_t           primaryTsIndex;
 
   // runtime state
   SGroupByColumnSupporter groupSup;
   SSHashObj              *pGroupProcessors;
 
 } SStreamWindowTriggerTask;
 
 typedef enum EStrwtWindowType {
   STRWT_WINDOW_TYPE_INTERVAL = 0,
   STRWT_WINDOW_TYPE_SESSION,
   STRWT_WINDOW_TYPE_STATE,
   STRWT_WINDOW_TYPE_EVENT,
   STRWT_WINDOW_TYPE_COUNT,
 } EStrwtWindowType;
 
 typedef struct SStrwtGroupProcessor {
   int64_t                   groupId;
   EStrwtWindowType          windowType;
   SStreamWindowTriggerTask *pTask;
 
   STimeWindow curWindow;
 } SStrwtGroupProcessor;
 
 typedef struct SStrwtIntervalGroupProcessor {
   SStrwtGroupProcessor base;
   SInterval            interval;
 } SStrwtIntervalGroupProcessor;
 
 typedef struct SStrwtSessionGroupProcessor {
   SStrwtGroupProcessor base;
   int64_t              gap;
 } SStrwtSessionGroupProcessor;
 
 typedef struct SStrwtStateGroupProcessor {
   SStrwtGroupProcessor base;
   SColumn              stateCol;
   SStateKeys           stateKey;
 } SStrwtStateGroupProcessor;
 
 typedef struct SStrwtEventGroupProcessor {
   SStrwtGroupProcessor base;
   SFilterInfo         *pStartCondInfo;
   SFilterInfo         *pEndCondInfo;
 } SStrwtEventGroupProcessor;
 
 typedef struct SStrwtCountGroupProcessor {
   SStrwtGroupProcessor base;
   int32_t              windowCount;
   int32_t              windowSliding;
   int32_t              numOfRows;
 } SStrwtCountGroupProcessor;
 
 static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
 
   switch (pProcessor->windowType) {
     case STRWT_WINDOW_TYPE_INTERVAL: {
       SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts <= pProcessor->curWindow.ekey);
       break;
     }
     case STRWT_WINDOW_TYPE_SESSION: {
       SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
       SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
       break;
     }
     case STRWT_WINDOW_TYPE_STATE: {
       SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
       SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
       QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
       if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
         *pRes = pState->stateKey.isNull;
       } else if (pState->stateKey.isNull) {
         *pRes = false;
       } else {
         char *val = colDataGetData(pStateColInfoData, rowIndex);
         *pRes = compareVal(val, &pState->stateKey);
       }
       break;
     }
     case STRWT_WINDOW_TYPE_EVENT:
     case STRWT_WINDOW_TYPE_COUNT: {
       *pRes = true;
       break;
     }
     default: {
       ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 
 static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
   SColumnInfoData          *ps = NULL, *pe = NULL;
 
   QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   int32_t nrows = pBlock->info.rows;
   if (nrows == 0) {
     goto _end;
   }
 
   if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
     // Check if data meets start and end conditions in batch
     SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
     SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
     int32_t            status = 0;
 
     code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
 
     code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
   for (int32_t i = 0; i < nrows; ++i) {
     if (IS_CUR_WIN_VALID(pProcessor)) {
       bool inWindow = false;
       code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
       QUERY_CHECK_CODE(code, lino, _end);
       if (!inWindow) {
         code = strwtgpCloseWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       }
     } else {
       bool shouldStart = true;
       if (ps != NULL) {
         shouldStart = ((bool*)ps->pData)[i];
       }
       if (shouldStart) {
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       } else {
         // ignore this data since it does not belong to any window
         continue;
       }
     }
     code = strwtgpUpdateState(pProcessor, pBlock, i);
     QUERY_CHECK_CODE(code, lino, _end);
     bool shouldEnd = false;
     if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
       SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
       shouldEnd = (pCount->windowCount == pCount->numOfRows);
     } else if (pe != NULL) {
       shouldEnd = ((bool*)pe->pData)[i];
     }
     if (shouldEnd) {
       code = strwtgpCloseWindow(pProcessor, pBlock, i);
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   if (pGroupKeys != NULL) {
     code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
   QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
   // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   if (nBlocks <= 0) {
     goto _end;
   }
 
   // todo(kjq): implement the window trigger process
   switch (pBlocks[0]->info.type) {
     case STREAM_NORMAL:
       // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first
 
     case STREAM_DELETE_DATA:
 
     default:
       ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   // todo(kjq): support other trigger types
   switch (pTask->type) {
     case STREAM_PERIODIC_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STERAM_COMMIT_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STREAM_WINDOW_TRIGGER:
       return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);
 
     default:
       ST_TASK_ELOG("invalid trigger type %d", pTask->type);
       return TSDB_CODE_INVALID_PARA;
   }
 }
 
 
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "streamTriggerTask.h"

 #include "executorInt.h"
 #include "filter.h"
 #include "tdatablock.h"
 
 #define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
 #define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN
 
 typedef struct SStreamWindowTriggerTask {
   SStreamTriggerTask base;
 
   // construct info
   SNodeList        *pGroupKeys;
   SWindowPhysiNode *pWindow;
   int32_t           primaryTsIndex;
 
   // runtime state
   SGroupByColumnSupporter groupSup;
   SSHashObj              *pGroupProcessors;
 
 } SStreamWindowTriggerTask;
 
 typedef enum EStrwtWindowType {
   STRWT_WINDOW_TYPE_INTERVAL = 0,
   STRWT_WINDOW_TYPE_SESSION,
   STRWT_WINDOW_TYPE_STATE,
   STRWT_WINDOW_TYPE_EVENT,
   STRWT_WINDOW_TYPE_COUNT,
 } EStrwtWindowType;
 
 typedef struct SStrwtGroupProcessor {
   int64_t                   groupId;
   EStrwtWindowType          windowType;
   SStreamWindowTriggerTask *pTask;
 
   STimeWindow curWindow;
 } SStrwtGroupProcessor;
 
 typedef struct SStrwtIntervalGroupProcessor {
   SStrwtGroupProcessor base;
   SInterval            interval;
 } SStrwtIntervalGroupProcessor;
 
 typedef struct SStrwtSessionGroupProcessor {
   SStrwtGroupProcessor base;
   int64_t              gap;
 } SStrwtSessionGroupProcessor;
 
 typedef struct SStrwtStateGroupProcessor {
   SStrwtGroupProcessor base;
   SColumn              stateCol;
   SStateKeys           stateKey;
 } SStrwtStateGroupProcessor;
 
 typedef struct SStrwtEventGroupProcessor {
   SStrwtGroupProcessor base;
   SFilterInfo         *pStartCondInfo;
   SFilterInfo         *pEndCondInfo;
 } SStrwtEventGroupProcessor;
 
 typedef struct SStrwtCountGroupProcessor {
   SStrwtGroupProcessor base;
   int32_t              windowCount;
   int32_t              windowSliding;
   int32_t              numOfRows;
 } SStrwtCountGroupProcessor;
 
 static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
 
   switch (pProcessor->windowType) {
     case STRWT_WINDOW_TYPE_INTERVAL: {
       SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts <= pProcessor->curWindow.ekey);
       break;
     }
     case STRWT_WINDOW_TYPE_SESSION: {
       SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
       SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
       break;
     }
     case STRWT_WINDOW_TYPE_STATE: {
       SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
       SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
       QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
       if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
         *pRes = pState->stateKey.isNull;
       } else if (pState->stateKey.isNull) {
         *pRes = false;
       } else {
         char *val = colDataGetData(pStateColInfoData, rowIndex);
         *pRes = compareVal(val, &pState->stateKey);
       }
       break;
     }
     case STRWT_WINDOW_TYPE_EVENT:
     case STRWT_WINDOW_TYPE_COUNT: {
       *pRes = true;
       break;
     }
     default: {
       ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 
 static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
   SColumnInfoData          *ps = NULL, *pe = NULL;
 
   QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   int32_t nrows = pBlock->info.rows;
   if (nrows == 0) {
     goto _end;
   }
 
   if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
     // Check if data meets start and end conditions in batch
     SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
     SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
     int32_t            status = 0;
 
     code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
 
     code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
   for (int32_t i = 0; i < nrows; ++i) {
     if (IS_CUR_WIN_VALID(pProcessor)) {
       bool inWindow = false;
       code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
       QUERY_CHECK_CODE(code, lino, _end);
       if (!inWindow) {
         code = strwtgpCloseWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       }
     } else {
       bool shouldStart = true;
       if (ps != NULL) {
         shouldStart = ((bool*)ps->pData)[i];
       }
       if (shouldStart) {
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       } else {
         // ignore this data since it does not belong to any window
         continue;
       }
     }
     code = strwtgpUpdateState(pProcessor, pBlock, i);
     QUERY_CHECK_CODE(code, lino, _end);
     bool shouldEnd = false;
     if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
       SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
       shouldEnd = (pCount->windowCount == pCount->numOfRows);
     } else if (pe != NULL) {
       shouldEnd = ((bool*)pe->pData)[i];
     }
     if (shouldEnd) {
       code = strwtgpCloseWindow(pProcessor, pBlock, i);
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   if (pGroupKeys != NULL) {
     code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
   QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
   // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   if (nBlocks <= 0) {
     goto _end;
   }
 
   // todo(kjq): implement the window trigger process
   switch (pBlocks[0]->info.type) {
     case STREAM_NORMAL:
       // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first
 
     case STREAM_DELETE_DATA:
 
     default:
       ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   // todo(kjq): support other trigger types
   switch (pTask->type) {
     case STREAM_PERIODIC_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STERAM_COMMIT_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STREAM_WINDOW_TRIGGER:
       return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);
 
     default:
       ST_TASK_ELOG("invalid trigger type %d", pTask->type);
       return TSDB_CODE_INVALID_PARA;
   }
 }
 
 
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "streamTriggerTask.h"

 #include "executorInt.h"
 #include "filter.h"
 #include "tdatablock.h"
 
 #define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
 #define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN
 
 typedef struct SStreamWindowTriggerTask {
   SStreamTriggerTask base;
 
   // construct info
   SNodeList        *pGroupKeys;
   SWindowPhysiNode *pWindow;
   int32_t           primaryTsIndex;
 
   // runtime state
   SGroupByColumnSupporter groupSup;
   SSHashObj              *pGroupProcessors;
 
 } SStreamWindowTriggerTask;
 
 typedef enum EStrwtWindowType {
   STRWT_WINDOW_TYPE_INTERVAL = 0,
   STRWT_WINDOW_TYPE_SESSION,
   STRWT_WINDOW_TYPE_STATE,
   STRWT_WINDOW_TYPE_EVENT,
   STRWT_WINDOW_TYPE_COUNT,
 } EStrwtWindowType;
 
 typedef struct SStrwtGroupProcessor {
   int64_t                   groupId;
   EStrwtWindowType          windowType;
   SStreamWindowTriggerTask *pTask;
 
   STimeWindow curWindow;
 } SStrwtGroupProcessor;
 
 typedef struct SStrwtIntervalGroupProcessor {
   SStrwtGroupProcessor base;
   SInterval            interval;
 } SStrwtIntervalGroupProcessor;
 
 typedef struct SStrwtSessionGroupProcessor {
   SStrwtGroupProcessor base;
   int64_t              gap;
 } SStrwtSessionGroupProcessor;
 
 typedef struct SStrwtStateGroupProcessor {
   SStrwtGroupProcessor base;
   SColumn              stateCol;
   SStateKeys           stateKey;
 } SStrwtStateGroupProcessor;
 
 typedef struct SStrwtEventGroupProcessor {
   SStrwtGroupProcessor base;
   SFilterInfo         *pStartCondInfo;
   SFilterInfo         *pEndCondInfo;
 } SStrwtEventGroupProcessor;
 
 typedef struct SStrwtCountGroupProcessor {
   SStrwtGroupProcessor base;
   int32_t              windowCount;
   int32_t              windowSliding;
   int32_t              numOfRows;
 } SStrwtCountGroupProcessor;
 
 static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
 
   switch (pProcessor->windowType) {
     case STRWT_WINDOW_TYPE_INTERVAL: {
       SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts <= pProcessor->curWindow.ekey);
       break;
     }
     case STRWT_WINDOW_TYPE_SESSION: {
       SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
       SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
       break;
     }
     case STRWT_WINDOW_TYPE_STATE: {
       SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
       SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
       QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
       if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
         *pRes = pState->stateKey.isNull;
       } else if (pState->stateKey.isNull) {
         *pRes = false;
       } else {
         char *val = colDataGetData(pStateColInfoData, rowIndex);
         *pRes = compareVal(val, &pState->stateKey);
       }
       break;
     }
     case STRWT_WINDOW_TYPE_EVENT:
     case STRWT_WINDOW_TYPE_COUNT: {
       *pRes = true;
       break;
     }
     default: {
       ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 
 static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
   SColumnInfoData          *ps = NULL, *pe = NULL;
 
   QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   int32_t nrows = pBlock->info.rows;
   if (nrows == 0) {
     goto _end;
   }
 
   if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
     // Check if data meets start and end conditions in batch
     SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
     SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
     int32_t            status = 0;
 
     code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
 
     code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
   for (int32_t i = 0; i < nrows; ++i) {
     if (IS_CUR_WIN_VALID(pProcessor)) {
       bool inWindow = false;
       code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
       QUERY_CHECK_CODE(code, lino, _end);
       if (!inWindow) {
         code = strwtgpCloseWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       }
     } else {
       bool shouldStart = true;
       if (ps != NULL) {
         shouldStart = ((bool*)ps->pData)[i];
       }
       if (shouldStart) {
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       } else {
         // ignore this data since it does not belong to any window
         continue;
       }
     }
     code = strwtgpUpdateState(pProcessor, pBlock, i);
     QUERY_CHECK_CODE(code, lino, _end);
     bool shouldEnd = false;
     if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
       SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
       shouldEnd = (pCount->windowCount == pCount->numOfRows);
     } else if (pe != NULL) {
       shouldEnd = ((bool*)pe->pData)[i];
     }
     if (shouldEnd) {
       code = strwtgpCloseWindow(pProcessor, pBlock, i);
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   if (pGroupKeys != NULL) {
     code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
   QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
   // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   if (nBlocks <= 0) {
     goto _end;
   }
 
   // todo(kjq): implement the window trigger process
   switch (pBlocks[0]->info.type) {
     case STREAM_NORMAL:
       // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first
 
     case STREAM_DELETE_DATA:
 
     default:
       ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   // todo(kjq): support other trigger types
   switch (pTask->type) {
     case STREAM_PERIODIC_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STERAM_COMMIT_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STREAM_WINDOW_TRIGGER:
       return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);
 
     default:
       ST_TASK_ELOG("invalid trigger type %d", pTask->type);
       return TSDB_CODE_INVALID_PARA;
   }
 }
 
 
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "streamTriggerTask.h"

 #include "executorInt.h"
 #include "filter.h"
 #include "tdatablock.h"
 
 #define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
 #define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN
 
 typedef struct SStreamWindowTriggerTask {
   SStreamTriggerTask base;
 
   // construct info
   SNodeList        *pGroupKeys;
   SWindowPhysiNode *pWindow;
   int32_t           primaryTsIndex;
 
   // runtime state
   SGroupByColumnSupporter groupSup;
   SSHashObj              *pGroupProcessors;
 
 } SStreamWindowTriggerTask;
 
 typedef enum EStrwtWindowType {
   STRWT_WINDOW_TYPE_INTERVAL = 0,
   STRWT_WINDOW_TYPE_SESSION,
   STRWT_WINDOW_TYPE_STATE,
   STRWT_WINDOW_TYPE_EVENT,
   STRWT_WINDOW_TYPE_COUNT,
 } EStrwtWindowType;
 
 typedef struct SStrwtGroupProcessor {
   int64_t                   groupId;
   EStrwtWindowType          windowType;
   SStreamWindowTriggerTask *pTask;
 
   STimeWindow curWindow;
 } SStrwtGroupProcessor;
 
 typedef struct SStrwtIntervalGroupProcessor {
   SStrwtGroupProcessor base;
   SInterval            interval;
 } SStrwtIntervalGroupProcessor;
 
 typedef struct SStrwtSessionGroupProcessor {
   SStrwtGroupProcessor base;
   int64_t              gap;
 } SStrwtSessionGroupProcessor;
 
 typedef struct SStrwtStateGroupProcessor {
   SStrwtGroupProcessor base;
   SColumn              stateCol;
   SStateKeys           stateKey;
 } SStrwtStateGroupProcessor;
 
 typedef struct SStrwtEventGroupProcessor {
   SStrwtGroupProcessor base;
   SFilterInfo         *pStartCondInfo;
   SFilterInfo         *pEndCondInfo;
 } SStrwtEventGroupProcessor;
 
 typedef struct SStrwtCountGroupProcessor {
   SStrwtGroupProcessor base;
   int32_t              windowCount;
   int32_t              windowSliding;
   int32_t              numOfRows;
 } SStrwtCountGroupProcessor;
 
 static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
 
   switch (pProcessor->windowType) {
     case STRWT_WINDOW_TYPE_INTERVAL: {
       SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts <= pProcessor->curWindow.ekey);
       break;
     }
     case STRWT_WINDOW_TYPE_SESSION: {
       SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
       SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
       break;
     }
     case STRWT_WINDOW_TYPE_STATE: {
       SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
       SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
       QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
       if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
         *pRes = pState->stateKey.isNull;
       } else if (pState->stateKey.isNull) {
         *pRes = false;
       } else {
         char *val = colDataGetData(pStateColInfoData, rowIndex);
         *pRes = compareVal(val, &pState->stateKey);
       }
       break;
     }
     case STRWT_WINDOW_TYPE_EVENT:
     case STRWT_WINDOW_TYPE_COUNT: {
       *pRes = true;
       break;
     }
     default: {
       ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 
 static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
   SColumnInfoData          *ps = NULL, *pe = NULL;
 
   QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   int32_t nrows = pBlock->info.rows;
   if (nrows == 0) {
     goto _end;
   }
 
   if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
     // Check if data meets start and end conditions in batch
     SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
     SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
     int32_t            status = 0;
 
     code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
 
     code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
   for (int32_t i = 0; i < nrows; ++i) {
     if (IS_CUR_WIN_VALID(pProcessor)) {
       bool inWindow = false;
       code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
       QUERY_CHECK_CODE(code, lino, _end);
       if (!inWindow) {
         code = strwtgpCloseWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       }
     } else {
       bool shouldStart = true;
       if (ps != NULL) {
         shouldStart = ((bool*)ps->pData)[i];
       }
       if (shouldStart) {
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       } else {
         // ignore this data since it does not belong to any window
         continue;
       }
     }
     code = strwtgpUpdateState(pProcessor, pBlock, i);
     QUERY_CHECK_CODE(code, lino, _end);
     bool shouldEnd = false;
     if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
       SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
       shouldEnd = (pCount->windowCount == pCount->numOfRows);
     } else if (pe != NULL) {
       shouldEnd = ((bool*)pe->pData)[i];
     }
     if (shouldEnd) {
       code = strwtgpCloseWindow(pProcessor, pBlock, i);
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   if (pGroupKeys != NULL) {
     code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
   QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
   // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   if (nBlocks <= 0) {
     goto _end;
   }
 
   // todo(kjq): implement the window trigger process
   switch (pBlocks[0]->info.type) {
     case STREAM_NORMAL:
       // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first
 
     case STREAM_DELETE_DATA:
 
     default:
       ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   // todo(kjq): support other trigger types
   switch (pTask->type) {
     case STREAM_PERIODIC_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STERAM_COMMIT_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STREAM_WINDOW_TRIGGER:
       return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);
 
     default:
       ST_TASK_ELOG("invalid trigger type %d", pTask->type);
       return TSDB_CODE_INVALID_PARA;
   }
 }
 
 
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "streamTriggerTask.h"

 #include "executorInt.h"
 #include "filter.h"
 #include "tdatablock.h"
 
 #define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
 #define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN
 
 typedef struct SStreamWindowTriggerTask {
   SStreamTriggerTask base;
 
   // construct info
   SNodeList        *pGroupKeys;
   SWindowPhysiNode *pWindow;
   int32_t           primaryTsIndex;
 
   // runtime state
   SGroupByColumnSupporter groupSup;
   SSHashObj              *pGroupProcessors;
 
 } SStreamWindowTriggerTask;
 
 typedef enum EStrwtWindowType {
   STRWT_WINDOW_TYPE_INTERVAL = 0,
   STRWT_WINDOW_TYPE_SESSION,
   STRWT_WINDOW_TYPE_STATE,
   STRWT_WINDOW_TYPE_EVENT,
   STRWT_WINDOW_TYPE_COUNT,
 } EStrwtWindowType;
 
 typedef struct SStrwtGroupProcessor {
   int64_t                   groupId;
   EStrwtWindowType          windowType;
   SStreamWindowTriggerTask *pTask;
 
   STimeWindow curWindow;
 } SStrwtGroupProcessor;
 
 typedef struct SStrwtIntervalGroupProcessor {
   SStrwtGroupProcessor base;
   SInterval            interval;
 } SStrwtIntervalGroupProcessor;
 
 typedef struct SStrwtSessionGroupProcessor {
   SStrwtGroupProcessor base;
   int64_t              gap;
 } SStrwtSessionGroupProcessor;
 
 typedef struct SStrwtStateGroupProcessor {
   SStrwtGroupProcessor base;
   SColumn              stateCol;
   SStateKeys           stateKey;
 } SStrwtStateGroupProcessor;
 
 typedef struct SStrwtEventGroupProcessor {
   SStrwtGroupProcessor base;
   SFilterInfo         *pStartCondInfo;
   SFilterInfo         *pEndCondInfo;
 } SStrwtEventGroupProcessor;
 
 typedef struct SStrwtCountGroupProcessor {
   SStrwtGroupProcessor base;
   int32_t              windowCount;
   int32_t              windowSliding;
   int32_t              numOfRows;
 } SStrwtCountGroupProcessor;
 
 static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
 
   switch (pProcessor->windowType) {
     case STRWT_WINDOW_TYPE_INTERVAL: {
       SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts <= pProcessor->curWindow.ekey);
       break;
     }
     case STRWT_WINDOW_TYPE_SESSION: {
       SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
       SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
       break;
     }
     case STRWT_WINDOW_TYPE_STATE: {
       SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
       SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
       QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
       if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
         *pRes = pState->stateKey.isNull;
       } else if (pState->stateKey.isNull) {
         *pRes = false;
       } else {
         char *val = colDataGetData(pStateColInfoData, rowIndex);
         *pRes = compareVal(val, &pState->stateKey);
       }
       break;
     }
     case STRWT_WINDOW_TYPE_EVENT:
     case STRWT_WINDOW_TYPE_COUNT: {
       *pRes = true;
       break;
     }
     default: {
       ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 
 static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
   SColumnInfoData          *ps = NULL, *pe = NULL;
 
   QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   int32_t nrows = pBlock->info.rows;
   if (nrows == 0) {
     goto _end;
   }
 
   if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
     // Check if data meets start and end conditions in batch
     SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
     SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
     int32_t            status = 0;
 
     code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
 
     code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
   for (int32_t i = 0; i < nrows; ++i) {
     if (IS_CUR_WIN_VALID(pProcessor)) {
       bool inWindow = false;
       code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
       QUERY_CHECK_CODE(code, lino, _end);
       if (!inWindow) {
         code = strwtgpCloseWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       }
     } else {
       bool shouldStart = true;
       if (ps != NULL) {
         shouldStart = ((bool*)ps->pData)[i];
       }
       if (shouldStart) {
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       } else {
         // ignore this data since it does not belong to any window
         continue;
       }
     }
     code = strwtgpUpdateState(pProcessor, pBlock, i);
     QUERY_CHECK_CODE(code, lino, _end);
     bool shouldEnd = false;
     if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
       SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
       shouldEnd = (pCount->windowCount == pCount->numOfRows);
     } else if (pe != NULL) {
       shouldEnd = ((bool*)pe->pData)[i];
     }
     if (shouldEnd) {
       code = strwtgpCloseWindow(pProcessor, pBlock, i);
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   if (pGroupKeys != NULL) {
     code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
   QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
   // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   if (nBlocks <= 0) {
     goto _end;
   }
 
   // todo(kjq): implement the window trigger process
   switch (pBlocks[0]->info.type) {
     case STREAM_NORMAL:
       // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first
 
     case STREAM_DELETE_DATA:
 
     default:
       ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   // todo(kjq): support other trigger types
   switch (pTask->type) {
     case STREAM_PERIODIC_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STERAM_COMMIT_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STREAM_WINDOW_TRIGGER:
       return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);
 
     default:
       ST_TASK_ELOG("invalid trigger type %d", pTask->type);
       return TSDB_CODE_INVALID_PARA;
   }
 }
 
 
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "streamTriggerTask.h"

 #include "executorInt.h"
 #include "filter.h"
 #include "tdatablock.h"
 
 #define SET_CUR_WIN_INVALID(pProcessor) (pProcessor)->curWindow.skey = INT64_MIN
 #define IS_CUR_WIN_VALID(pProcessor)    (pProcessor)->curWindow.skey != INT64_MIN
 
 typedef struct SStreamWindowTriggerTask {
   SStreamTriggerTask base;
 
   // construct info
   SNodeList        *pGroupKeys;
   SWindowPhysiNode *pWindow;
   int32_t           primaryTsIndex;
 
   // runtime state
   SGroupByColumnSupporter groupSup;
   SSHashObj              *pGroupProcessors;
 
 } SStreamWindowTriggerTask;
 
 typedef enum EStrwtWindowType {
   STRWT_WINDOW_TYPE_INTERVAL = 0,
   STRWT_WINDOW_TYPE_SESSION,
   STRWT_WINDOW_TYPE_STATE,
   STRWT_WINDOW_TYPE_EVENT,
   STRWT_WINDOW_TYPE_COUNT,
 } EStrwtWindowType;
 
 typedef struct SStrwtGroupProcessor {
   int64_t                   groupId;
   EStrwtWindowType          windowType;
   SStreamWindowTriggerTask *pTask;
 
   STimeWindow curWindow;
 } SStrwtGroupProcessor;
 
 typedef struct SStrwtIntervalGroupProcessor {
   SStrwtGroupProcessor base;
   SInterval            interval;
 } SStrwtIntervalGroupProcessor;
 
 typedef struct SStrwtSessionGroupProcessor {
   SStrwtGroupProcessor base;
   int64_t              gap;
 } SStrwtSessionGroupProcessor;
 
 typedef struct SStrwtStateGroupProcessor {
   SStrwtGroupProcessor base;
   SColumn              stateCol;
   SStateKeys           stateKey;
 } SStrwtStateGroupProcessor;
 
 typedef struct SStrwtEventGroupProcessor {
   SStrwtGroupProcessor base;
   SFilterInfo         *pStartCondInfo;
   SFilterInfo         *pEndCondInfo;
 } SStrwtEventGroupProcessor;
 
 typedef struct SStrwtCountGroupProcessor {
   SStrwtGroupProcessor base;
   int32_t              windowCount;
   int32_t              windowSliding;
   int32_t              numOfRows;
 } SStrwtCountGroupProcessor;
 
 static int32_t strwtgpInCurWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex, bool *pRes) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
 
   switch (pProcessor->windowType) {
     case STRWT_WINDOW_TYPE_INTERVAL: {
       SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY            ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts <= pProcessor->curWindow.ekey);
       break;
     }
     case STRWT_WINDOW_TYPE_SESSION: {
       SStrwtSessionGroupProcessor *pSession = (SStrwtSessionGroupProcessor *)pProcessor;
       SColumnInfoData             *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
       TSKEY                        ts = ((TSKEY *)pTsCol->pData)[rowIndex];
       *pRes = (ts < (pProcessor->curWindow.ekey + pSession->gap));
       break;
     }
     case STRWT_WINDOW_TYPE_STATE: {
       SStrwtStateGroupProcessor *pState = (SStrwtStateGroupProcessor *)pProcessor;
       SColumnInfoData           *pStateColInfoData = taosArrayGet(pBlock->pDataBlock, pState->stateCol.slotId);
       QUERY_CHECK_NULL(pStateColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
       if (colDataIsNull_s(pStateColInfoData, rowIndex)) {
         *pRes = pState->stateKey.isNull;
       } else if (pState->stateKey.isNull) {
         *pRes = false;
       } else {
         char *val = colDataGetData(pStateColInfoData, rowIndex);
         *pRes = compareVal(val, &pState->stateKey);
       }
       break;
     }
     case STRWT_WINDOW_TYPE_EVENT:
     case STRWT_WINDOW_TYPE_COUNT: {
       *pRes = true;
       break;
     }
     default: {
       ST_TASK_ELOG("unknown window type %d", pProcessor->windowType);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 static int32_t strwtgpOpenWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpCloseWindow(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 static int32_t strwtgpUpdateState(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock, int32_t rowIndex);
 
 static int32_t strwtgpProcessData(SStrwtGroupProcessor *pProcessor, SSDataBlock *pBlock) {
   int32_t                   code = TSDB_CODE_SUCCESS;
   int32_t                   lino = 0;
   SStreamWindowTriggerTask *pTask = pProcessor->pTask;
   SColumnInfoData          *ps = NULL, *pe = NULL;
 
   QUERY_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   int32_t nrows = pBlock->info.rows;
   if (nrows == 0) {
     goto _end;
   }
 
   if (pProcessor->windowType == STRWT_WINDOW_TYPE_EVENT) {
     // Check if data meets start and end conditions in batch
     SStrwtEventGroupProcessor *pEvent = (SStrwtEventGroupProcessor *)pProcessor;
     SFilterColumnParam param = {.numOfCols = taosArrayGetSize(pBlock->pDataBlock), .pDataBlock = pBlock->pDataBlock};
     int32_t            status = 0;
 
     code = filterSetDataFromSlotId(pEvent->pStartCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pStartCondInfo, pBlock, &ps, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
 
     code = filterSetDataFromSlotId(pEvent->pEndCondInfo, &param);
     QUERY_CHECK_CODE(code, lino, _end);
     code = filterExecute(pEvent->pEndCondInfo, pBlock, &pe, NULL, param.numOfCols, &status);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, pTask->primaryTsIndex);
   for (int32_t i = 0; i < nrows; ++i) {
     if (IS_CUR_WIN_VALID(pProcessor)) {
       bool inWindow = false;
       code = strwtgpInCurWindow(pProcessor, pBlock, i, &inWindow);
       QUERY_CHECK_CODE(code, lino, _end);
       if (!inWindow) {
         code = strwtgpCloseWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       }
     } else {
       bool shouldStart = true;
       if (ps != NULL) {
         shouldStart = ((bool*)ps->pData)[i];
       }
       if (shouldStart) {
         code = strwtgpOpenWindow(pProcessor, pBlock, i);
         QUERY_CHECK_CODE(code, lino, _end);
       } else {
         // ignore this data since it does not belong to any window
         continue;
       }
     }
     code = strwtgpUpdateState(pProcessor, pBlock, i);
     QUERY_CHECK_CODE(code, lino, _end);
     bool shouldEnd = false;
     if (pProcessor->windowType == STRWT_WINDOW_TYPE_COUNT) {
       SStrwtCountGroupProcessor *pCount = (SStrwtCountGroupProcessor *)pProcessor;
       shouldEnd = (pCount->windowCount == pCount->numOfRows);
     } else if (pe != NULL) {
       shouldEnd = ((bool*)pe->pData)[i];
     }
     if (shouldEnd) {
       code = strwtgpCloseWindow(pProcessor, pBlock, i);
       QUERY_CHECK_CODE(code, lino, _end);
     }
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtInit(SStreamWindowTriggerTask *pTask, SNodeList *pGroupKeys, const char *idstr) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   if (pGroupKeys != NULL) {
     code = grpByColSupInit(&pTask->groupSup, pGroupKeys);
     QUERY_CHECK_CODE(code, lino, _end);
   }
 
   pTask->pGroupProcessors = tSimpleHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
   QUERY_CHECK_NULL(pTask->pGroupProcessors, code, lino, _end, terrno);
   // tSimpleHashSetFreeFp(pTask->pGroupProcessors, _hash_free_fn_t freeFp);
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strwtProcessData(SStreamWindowTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   int32_t code = TSDB_CODE_SUCCESS;
   int32_t lino = 0;
 
   QUERY_CHECK_CONDITION(nBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
 
   if (nBlocks <= 0) {
     goto _end;
   }
 
   // todo(kjq): implement the window trigger process
   switch (pBlocks[0]->info.type) {
     case STREAM_NORMAL:
       // todo(kjq): if it's a virtual table, merge the original table data into virtual table data first
 
     case STREAM_DELETE_DATA:
 
     default:
       ST_TASK_ELOG("invalid data type %d", pBlocks[0]->info.type);
       code = TSDB_CODE_INVALID_PARA;
       QUERY_CHECK_CODE(code, lino, _end);
   }
 
 _end:
   if (code != TSDB_CODE_SUCCESS) {
     ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
   }
   return code;
 }
 
 int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks) {
   // todo(kjq): support other trigger types
   switch (pTask->type) {
     case STREAM_PERIODIC_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STERAM_COMMIT_TRIGGER:
       return TSDB_CODE_OPS_NOT_SUPPORT;
 
     case STREAM_WINDOW_TRIGGER:
       return strwtProcessData((SStreamWindowTriggerTask *)pTask, pBlocks, nBlocks);
 
     default:
       ST_TASK_ELOG("invalid trigger type %d", pTask->type);
       return TSDB_CODE_INVALID_PARA;
   }
 }
 
 
       