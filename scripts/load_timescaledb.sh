#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-timescaledb-data.gz}
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-10s}
CHUNK_TIME=${CHUNK_TIME:-8h}
PARTITIONS=${PARTITIONS:-1}
PERF_OUTPUT=${PERF_OUTPUT:-}
DATABASE_HOST=${DATABASE_HOST:-localhost}
DATABASE_USER=${DATABASE_USER:-postgres}

source ${EXE_DIR}/load_common.sh
source ${EXE_DIR}/timescaledb.conf

while ! pg_isready -h ${DATABASE_HOST}; do
    echo "Waiting for timescaledb"
    sleep 1
done

cat ${DATA_FILE} | gunzip | tsbs_load_timescaledb \
                                --postgres="sslmode=disable" \
                                --db-name=${DATABASE_NAME} \
                                --host=${DATABASE_HOST} \
                                --user=${DATABASE_USER} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${PROGRESS_INTERVAL} \
                                --use-hypertable=${USE_HYPERTABLE} \
                                --use-jsonb-tags=${JSON_TAGS} \
                                --in-table-partition-tag=${IN_TABLE_PARTITION_TAG} \
                                --partitions=${PARTITIONS} \
                                --chunk-time=${CHUNK_TIME} \
                                --write-profile=${PERF_OUTPUT} \
                                --field-index-count=1
