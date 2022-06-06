#!/usr/bin/env bash

# This script assumes questdb is up and running, you can start it with docker with
# docker run -p 9000:9000 -p 8812:8812 -p 9009:9009 -p 9003:9003 questdb/questdb


# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_questdb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_questdb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-influx-data.gz}
DATABASE_PORT=${DATABASE_PORT:-9000}
DATABASE_HEALTH_PORT=${DATABASE_HEALTH_PORT:-9003}
ILP_PORT=${ILP_PORT:-9009}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

until curl http://${DATABASE_HOST}:${DATABASE_HEALTH_PORT}/ping 2>/dev/null; do
    echo "Waiting for QuestDB"
    sleep 1
done

# Remove previous table
curl -X GET http://${DATABASE_HOST}:${DATABASE_PORT}/exec?query=drop%20table%20cpu
# Load new data
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --url=http://${DATABASE_HOST}:${DATABASE_PORT} \
                                --ilp-bind-to ${DATABASE_HOST}:${ILP_PORT}
