#!/bin/bash
set -x
EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-cassandra.gz}
CASSANDRA_BATCH_SIZE=${CASSANDRA_BATCH_SIZE:-100}
source ${EXE_DIR}/load_common.sh

cat ${DATA_FILE} | gunzip | bulk_load_cassandra \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${CASSANDRA_BATCH_SIZE} \
                                --url=${DATABASE_HOST}:9042 