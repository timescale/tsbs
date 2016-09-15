#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/query_common.sh

source ${EXE_DIR}/iobeamdb.conf

cat ${QUERIES_FILE} | gunzip | query_benchmarker_iobeam \
                                   --workers=${NUM_WORKERS} \
                                   --postgres="$POSTGRES_CONNECT dbname=$POSTGRES_DB"
