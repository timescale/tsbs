#!/bin/bash
EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-cassandra-data.gz}
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-20s}
CASSANDRA_TIMEOUT=${CASSANDRA_TIMEOUT:-1000s}
DATABASE_PORT=${DATABASE_PORT:-9042}
source ${EXE_DIR}/load_common.sh
BATCH_SIZE=${BATCH_SIZE:-100}

while ! nc -z ${DATABASE_HOST} ${DATABASE_PORT}; do
    echo "Waiting for cassandra"
    sleep 1
done

cqlsh -e 'drop keyspace measurements;'
cat ${DATA_FILE} | gunzip | tsbs_load_cassandra \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${PROGRESS_INTERVAL} \
                                --write-timeout=${CASSANDRA_TIMEOUT} \
                                --url=${DATABASE_HOST}:${DATABASE_PORT}
