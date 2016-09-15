#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/query_common.sh

source ${EXE_DIR}/iobeamdb.conf

until ssh ${DATABASE_HOST} docker exec postgres gosu postgres pg_ctl status 2>&1 >/dev/null; do
    echo "Waiting for ${target}"
    sleep 1
done

cat ${QUERIES_FILE} | gunzip | query_benchmarker_iobeam \
				   --print-responses=${PRINT_RESPONSES} \
                                   --workers=${NUM_WORKERS} \
                                   --postgres="$POSTGRES_CONNECT dbname=$POSTGRES_DB"
