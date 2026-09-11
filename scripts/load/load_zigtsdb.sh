#!/usr/bin/env bash

# This script assumes zigtsdb is up and running, e.g. (pin the digest from the release page):
#   docker run -d -p 8080:8080 -e TSDB_EXPECTED_SERIES=40000 ghcr.io/samisoftb/zigtsdb@sha256:<digest>
# The free tier meters 1M points/day; export ZIGTSDB_LICENSE_KEY to activate a key first.

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_zigtsdb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_zigtsdb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-zigtsdb-data.gz}
DATABASE_PORT=${DATABASE_PORT:-8080}
INGEST_PATH=${INGEST_PATH:-/dispatch/ingest/simd?precision=ns}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

ZIGTSDB_URL="http://${DATABASE_HOST}:${DATABASE_PORT}"

until curl -sf ${ZIGTSDB_URL}/health >/dev/null 2>&1; do
    echo "Waiting for zigtsdb"
    sleep 1
done

if [[ -n "${ZIGTSDB_LICENSE_KEY}" ]]; then
    curl -s -X POST ${ZIGTSDB_URL}/license/activate \
        -H 'Content-Type: application/json' \
        -d "{\"key\":\"${ZIGTSDB_LICENSE_KEY}\"}"
    echo
fi

# Load new data (the loader deletes existing TSBS metrics when --do-create-db is set)
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --do-create-db=${DO_CREATE_DB} \
                                --urls=${ZIGTSDB_URL} \
                                --ingest-path="${INGEST_PATH}"
