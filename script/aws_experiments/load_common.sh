#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATABASE_HOST=${DATABASE_HOST:-database}

# load parameters
BATCH_SIZE=${BATCH_SIZE:-1000}
SCALE_VAR=${SCALE_VAR:-32}
BULK_DATA_DIR=${BULK_DATA_DIR:-${DATA_DIR}/1}
NUM_WORKERS=${NUM_WORKERS:-4}
DATA_FILE=${BULK_DATA_DIR}/${DATA_FILE_NAME}
BACKOFF_SECS=${BACKOFF_SECS:-1s}

if [ ! -f ${DATA_FILE} ]; then
   echo "Cannot find data file ${DATA_FILE}"
   exit -1
fi
   
echo "Bulk loading file ${DATA_FILE}"

set -x
