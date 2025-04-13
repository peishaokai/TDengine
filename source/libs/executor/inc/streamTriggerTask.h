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

#ifndef TDENGINE_TSTREAM_H
#define TDENGINE_TSTREAM_H

#include "stream.h"
#include "tcommon.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum EStreamTriggerType {
  STREAM_PERIODIC_TRIGGER = 0,
  STREAM_WINDOW_TRIGGER,
} EStreamTriggerType;

typedef struct SStreamTriggerTask {
  SStreamTask        base;
  EStreamTriggerType type;
} SStreamTriggerTask;

/**
 * @brief Process data blocks received from reader. Trigger notifications and calculation requests.
 *
 * @param pTask Trigger task for the current stream, idstr must not be NULL
 * @param pBlocks Data blocks to be processed, these data must come from the same commit
 * @param nBlocks Number of data blocks to be processed
 * @return int32_t 0 on success, error code otherwise
 */
int32_t strtProcessData(SStreamTriggerTask *pTask, SSDataBlock **pBlocks, int32_t nBlocks);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_TSTREAM_H */
