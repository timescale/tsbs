#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-timescaledb-data.gz}
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-20s}
CHUNK_TIME=${CHUNK_TIME:-8h}
PARTITIONS=${PARTITIONS:-1}
USE_HYPERTABLE=${USE_HYPERTABLE:-true}

source ${EXE_DIR}/load_common.sh
source ${EXE_DIR}/timescaledb.conf

while ! pg_isready; do
    echo "Waiting for timescaledb"
    sleep 1
done

cat ${DATA_FILE} | gunzip | bulk_load_timescaledb \
                                --postgres="host=${DATABASE_HOST} user=postgres sslmode=disable" \
                                --db-name=${DATABASE_NAME} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${PROGRESS_INTERVAL} \
                                --use-hypertable=${USE_HYPERTABLE} \
                                --use-jsonb-tags=${JSON_TAGS} \
                                --in-table-partition-tag=${IN_TABLE_PARTITION_TAG} \
                                --partitions=${PARTITIONS} \
                                --chunk-time=${CHUNK_TIME} \
                                --field-index="VALUE-TIME" \
                                --field-index-count=1 \
                                --tag-index=""
