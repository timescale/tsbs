#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

EXE_FILE_NAME=./bin/tsbs_run_queries_redistimeseries

#set -x

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/../query_common.sh
source ${EXE_DIR}/../redistimeseries_common.sh

# Ensure RESULTS DIR available
mkdir -p ${RESULTS_DIR}

for FULL_DATA_FILE_NAME in ${BULK_DATA_DIR}/queries_${USE_CASE}_${FORMAT}_${SCALE}_*; do
  for run in $(seq ${REPETITIONS}); do

    DATA_FILE_NAME=$(basename -- "${FULL_DATA_FILE_NAME}")
    OUT_FULL_FILE_NAME="${RESULTS_DIR}/${PREFIX}_result_${DATA_FILE_NAME}_${run}.out"
    HDR_FULL_FILE_NAME="${RESULTS_DIR}/${PREFIX}_HDR_TXT_result_${DATA_FILE_NAME}_${run}.out"

    $EXE_FILE_NAME \
      --file $FULL_DATA_FILE_NAME \
      --max-queries=${MAX_QUERIES} \
      --workers=${NUM_WORKERS} \
      --print-interval=${QUERIES_PRINT_INTERVAL} \
      --debug=${DEBUG} \
      --hdr-latencies=${HDR_FULL_FILE_NAME} \
      --host=${DATABASE_HOST}:${DATABASE_PORT} ${CLUSTER_FLAG} |
      tee $OUT_FULL_FILE_NAME

    echo "Sleeping for ${SLEEP_BETWEEN_RUNS} seconds"
    sleep ${SLEEP_BETWEEN_RUNS}
  done
  echo "Sleeping for ${SLEEP_BETWEEN_RUNS} seconds"
  sleep ${SLEEP_BETWEEN_RUNS}

done
