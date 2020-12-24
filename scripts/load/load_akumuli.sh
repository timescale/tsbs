#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_akumuli)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_akumuli not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-akumuli-data.gz}
INGESTION_PORT=${INGESTION_PORT:-8282}
QUERY_PORT=${QUERY_PORT:-8181}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

until curl http://${DATABASE_HOST}:${QUERY_PORT}/api/stats 2>/dev/null; do
    echo "Waiting for akumulid"
    sleep 1
done

# Load new data
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --endpoint=${DATABASE_HOST}:${INGESTION_PORT}
