#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATA_FILE_NAME=${DATA_FILE_NAME:-iobeam-data.gz}
SQL_DIR=${SQL_DIR:-"${GOPATH}/src/bitbucket.org/440-labs/postgres-kafka-consumer/sql/scripts/"}

source ${EXE_DIR}/load_common.sh
source ${EXE_DIR}/iobeamdb.conf

pushd $SQL_DIR
COMMIT=$(git rev-parse HEAD)
BRANCH=$(git branch)
popd

echo "Using commit (for sql scripts inside postgres-kafka-consumer): $COMMIT ON branch $BRANCH" 

until ssh ${DATABASE_HOST} docker exec postgres gosu postgres pg_ctl status 2>&1 >/dev/null; do
    echo "Waiting for ${target}"
    sleep 1
done

cat ${DATA_FILE} | gunzip | bulk_load_iobeam \
                                --scriptsDir="$SQL_DIR" \
                                --postgres="$POSTGRES_CONNECT" \
                                --tag-index="VALUE-TIME" \
                                --field-index="" \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE}

