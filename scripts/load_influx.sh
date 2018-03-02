#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-influx-data.gz}
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-20s}
DATABASE_NAME=${DATABASE_NAME:-"benchmark"}
DATABASE_PORT=${DATABASE_PORT:-8086}
source ${EXE_DIR}/load_common.sh

until curl http://${DATABASE_HOST}:${DATABASE_PORT}/ping 2>/dev/null; do
    echo "Waiting for InfluxDB"
    sleep 1
done

# Remove previous database
curl -X POST http://${DATABASE_HOST}:${DATABASE_PORT}/query?q=drop%20database%20${DATABASE_NAME}
# Load new data
cat ${DATA_FILE} | gunzip | tsbs_load_influx \
                                --db-name=${DATABASE_NAME} \
                                --backoff=${BACKOFF_SECS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${PROGRESS_INTERVAL} \
                                --urls=http://${DATABASE_HOST}:${DATABASE_PORT}
