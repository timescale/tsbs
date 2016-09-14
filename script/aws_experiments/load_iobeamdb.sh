#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-iobeamdb.gz}
SQL_DIR=${SQL_DIR:-"${GOPATH}/bitbucket.org/440-labs/postgres-kafka-consumer/sql/scripts/"}

source ${EXE_DIR}/load_common.sh
source ${EXE_DIR}/postgres.conf


pushd $SQL_DIR
COMMIT=$(git rev-parse HEAD)
popd

echo "Using commit $COMMIT"

cat ${DATA_FILE} | gunzip | bulk_load_iobeam \
                                --scriptsDir="${GOPATH}/bitbucket.org/440-labs/postgres-kafka-consumer/sql/scripts/" \
                                --postgres="$POSTGRES_CONNECT" \
                                --tag-index="VALUE-TIME" \
                                --field-index="" \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE}

/disk/1/iobeam/data.gz | gunzip | ./bulk_load_iobeam \
