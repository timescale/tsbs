#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_cassandra)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_cassandra not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-cassandra-data.gz}
DATABASE_PORT=${DATABASE_PORT:-9042}

# Load parameters - personal
CASSANDRA_TIMEOUT=${CASSANDRA_TIMEOUT:-1000s}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}
BATCH_SIZE=${BATCH_SIZE:-100}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

while ! nc -z ${DATABASE_HOST} ${DATABASE_PORT}; do
    echo "Waiting for cassandra"
    sleep 1
done

cqlsh -e 'drop keyspace measurements;'
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --write-timeout=${CASSANDRA_TIMEOUT} \
                                --hosts=${DATABASE_HOST}:${DATABASE_PORT} \
                                --replication-factor=${REPLICATION_FACTOR}
