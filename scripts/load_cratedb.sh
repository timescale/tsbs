#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_cratedb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_cratedb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-cratedb-data.gz}
DATABASE_HOST=${DATABASE_HOST:-"localhost"}
DATABASE_PORT=${DATABASE_PORT:-5432}

# Load parameters - database specific
REPLICATION_FACTOR=${REPLICATION_FACTOR:-0}
NUMBER_OF_SHARDS=${NUMBER_OF_SHARDS:-5}
USER=${USER:-crate}
PASSWORD=${PASSWORD}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

while ! nc -z ${DATABASE_HOST} ${DATABASE_PORT}; do
    echo "Waiting for CrateDB..."
    sleep 1
done

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --hosts=${DATABASE_HOST} \
                                --port=${DATABASE_PORT} \
                                --user=${USER} \
                                --pass=${PASSWORD} \
                                --replicas=${REPLICATION_FACTOR} \
                                --shards=${NUMBER_OF_SHARDS}
