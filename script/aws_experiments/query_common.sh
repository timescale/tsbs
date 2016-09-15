#!/bin/bash

EXE_DIR=${EXE_DIR:-$(dirname $0)}
DATABASE_HOST=${DATABASE_HOST:-database}
DATA_DIR=${DATA_DIR:-/disk}

# query parameters
QUERIES_DATA_DIR=${QUERIES_DATA_DIR:-${DATA_DIR}/1/queries}
NUM_WORKERS=${NUM_WORKERS:-4}
QUERIES_FILE=${QUERIES_FILE:-${QUERIES_DATA_DIR}/${QUERIES_FILE_NAME}}

if [ ! -f ${QUERIES_FILE} ]; then
   echo "Cannot find queries file ${QUERIES_FILE}"
   exit -1
fi
   
echo "Running queries from file ${QUERIES_FILE}"

set -x
