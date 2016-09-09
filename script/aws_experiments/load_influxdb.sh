#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-influx-bulk.gz}
source ${EXE_DIR}/load_common.sh

cat ${DATA_FILE} | gunzip | bulk_load_influx \
                                --backoff=${BACKOFF_SECS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --url=http://${DATABASE_HOST}:8086
