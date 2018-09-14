#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_timescaledb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_timescaledb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-timescaledb-data.gz}
DATABASE_USER=${DATABASE_USER:-postgres}

# Load parameters - personal
CHUNK_TIME=${CHUNK_TIME:-8h}
PARTITIONS=${PARTITIONS:-1}
HASH_WORKERS=${HASH_WORKERS:-false}
TIME_PARTITION_INDEX=${TIME_PARTITION_INDEX:-false}
PERF_OUTPUT=${PERF_OUTPUT:-}
JSON_TAGS=${JSON_TAGS:-false}
IN_TABLE_PARTITION_TAG=${IN_TABLE_PARTITION_TAG:-true}
USE_HYPERTABLE=${USE_HYPERTABLE:-true}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

while ! pg_isready -h ${DATABASE_HOST}; do
    echo "Waiting for timescaledb"
    sleep 1
done

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --postgres="sslmode=disable" \
                                --db-name=${DATABASE_NAME} \
                                --host=${DATABASE_HOST} \
                                --user=${DATABASE_USER} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --use-hypertable=${USE_HYPERTABLE} \
                                --use-jsonb-tags=${JSON_TAGS} \
                                --in-table-partition-tag=${IN_TABLE_PARTITION_TAG} \
                                --hash-workers=${HASH_WORKERS} \
                                --time-partition-index=${TIME_PARTITION_INDEX} \
                                --partitions=${PARTITIONS} \
                                --chunk-time=${CHUNK_TIME} \
                                --write-profile=${PERF_OUTPUT} \
                                --field-index-count=1
