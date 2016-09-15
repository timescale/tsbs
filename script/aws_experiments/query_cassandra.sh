#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
QUERIES_FILE_NAME=${QUERIES_FILE_NAME:-cassandra-8-hosts-queries.gz}
source ${EXE_DIR}/query_common.sh

cat ${QUERIES_FILE} | gunzip | query_benchmarker_cassandra \
                                   --workers=${NUM_WORKERS} \
                                   --url=${DATABASE_HOST}:9042
