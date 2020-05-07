#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_clickhouse)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_clickhouse not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-clickhouse-data.gz}
DATABASE_USER=${DATABASE_USER:-default}
DATABASE_PASSWORD=${DATABASE_PASSWORD:-""}

# Load parameters - personal
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-10s}
HASH_WORKERS=${HASH_WORKERS:-false}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --host=${DATABASE_HOST} \
                                --port=${DATABASE_PORT} \
                                --user=${DATABASE_USER} \
                                --password=${DATABASE_PASSWORD} \
                                --db-name=${DATABASE_NAME} \
                                --batch-size=${BATCH_SIZE} \
                                --workers=${NUM_WORKERS} \
                                --reporting-period=${PROGRESS_INTERVAL} \
                                --hash-workers=${HASH_WORKERS}
