#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-timescaledb-data.gz}
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-20000}

source ${EXE_DIR}/load_common.sh
source ${EXE_DIR}/timescaledb.conf

while ! pg_isready; do
    echo "Waiting for timescaledb"
    sleep 1
done

cat ${DATA_FILE} | gunzip | bulk_load_timescaledb \
                                --batch-size ${BATCH_SIZE} \
                                --field-index="VALUE-TIME" \
                                --field-index-count=1 \
                                --make-hypertable=true \
                                --number_partitions 1 \
                                --jsonb-tags=${JSON_TAGS} \
                                --workers ${NUM_WORKERS} \
                                --db-name=${DATABASE_NAME} \
                                --postgres "host=${DATABASE_HOST} user=postgres sslmode=disable" \
                                --reporting-period ${PROGRESS_INTERVAL} \
                                --tag-index="" \
                                --batch-size=${BATCH_SIZE}
