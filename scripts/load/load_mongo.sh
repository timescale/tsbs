#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_mongo)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_mongo not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-mongo-data.gz}

# Load parameters - personal
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-10s}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

MONGO_URL=${MONGO_URL:-"mongodb://localhost:27017/"}
DOC_PER=${DOC_PER:-false}
TIMESERIES_COLLECTION=${TIMESERIES_COLLECTION:-false}
RETRYABLE_WRITES=${RETRYABLE_WRITES:-true}
ORDERED_INSERTS=${ORDERED_INSERTS:-true}
RANDOM_FIELD_ORDER=${RANDOM_FIELD_ORDER:-false}

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --url=${MONGO_URL} \
                                --batch-size=${BATCH_SIZE} \
                                --workers=${NUM_WORKERS} \
                                --document-per-event=${DOC_PER} \
                                --timeseries-collection=${TIMESERIES_COLLECTION} \
                                --retryable-writes=${RETRYABLE_WRITES} \
                                --ordered-inserts=${ORDERED_INSERTS} \
                                --random-field-order=${RANDOM_FIELD_ORDER} \
                                --reporting-period=${PROGRESS_INTERVAL}
