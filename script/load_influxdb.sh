#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-influx-bulk-data.gz}
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-20s}
source ${EXE_DIR}/load_common.sh

until curl http://${DATABASE_HOST}:8086/ping 2>/dev/null; do
    echo "Waiting for InfluxDB"
    sleep 1
done

# Remove previous database
curl -X POST http://${DATABASE_HOST}:8086/query?q=drop%20database%20benchmark_db
# Load new data
cat ${DATA_FILE} | gunzip | bulk_load_influx \
                                --backoff=${BACKOFF_SECS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
				--progress-interval=${PROGRESS_INTERVAL} \
                                --urls=http://${DATABASE_HOST}:8086
