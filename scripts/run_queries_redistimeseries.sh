#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Ensure runner is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_run_queries_redistimeseries)}
if [[ -z "$EXE_FILE_NAME" ]]; then
  echo "tsbs_run_queries_redistimeseries not available. It is not specified explicitly and not found in \$PATH"
  exit 1
fi

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/query_common.sh

DATABASE_PORT=${DATABASE_PORT:-6379}

# Print timing stats to stderr after this many queries (0 to disable)
QUERIES_PRINT_INTERVAL=${QUERIES_PRINT_INTERVAL:-"0"}

# How many queries would be run
MAX_QUERIES=${MAX_QUERIES:-"0"}
REPETITIONS=${REPETITIONS:-3}
PREFIX=${PREFIX:-""}

# How many queries would be run
SLEEP_BETWEEN_RUNS=${SLEEP_BETWEEN_RUNS:-"60"}

# Ensure DATA DIR available
mkdir -p ${RESULTS_DIR}
chmod a+rwx ${RESULTS_DIR}

for run in $(seq ${REPETITIONS}); do
  for FULL_DATA_FILE_NAME in ${BULK_DATA_DIR}/queries_redistimeseries*; do
    # $FULL_DATA_FILE_NAME:  /full/path/to/file_with.ext
    # $DATA_FILE_NAME:       file_with.ext
    # $DIR:                  /full/path/to
    # $EXTENSION:            ext
    # NO_EXT_DATA_FILE_NAME: file_with

    DATA_FILE_NAME=$(basename -- "${FULL_DATA_FILE_NAME}")
    DIR=$(dirname "${FULL_DATA_FILE_NAME}")
    EXTENSION="${DATA_FILE_NAME##*.}"
    NO_EXT_DATA_FILE_NAME="${DATA_FILE_NAME%.*}"

    OUT_FULL_FILE_NAME="${RESULTS_DIR}/${PREFIX}_result_${NO_EXT_DATA_FILE_NAME}_${run}.out"
    HDR_FULL_FILE_NAME="${RESULTS_DIR}/${PREFIX}_HDR_TXT_result_${NO_EXT_DATA_FILE_NAME}_${run}.out"

    if [ "${EXTENSION}" == "gz" ]; then
      GUNZIP="gunzip"
    else
      GUNZIP="cat"
    fi

    echo "Reseting Redis command stats"
    redis-cli -h ${DATABASE_HOST} -p ${DATABASE_PORT} config resetstat

    echo "Running ${DATA_FILE_NAME}"
    echo "Saving output to ${OUT_FULL_FILE_NAME}"
    echo "Saving HDR Latencies to ${HDR_FULL_FILE_NAME}"

    cat $FULL_DATA_FILE_NAME |
      $GUNZIP |
      $EXE_FILE_NAME \
        --max-queries=${MAX_QUERIES} \
        --workers=${NUM_WORKERS} \
        --print-interval=${QUERIES_PRINT_INTERVAL} \
        --debug=${DEBUG} \
        --hdr-latencies=${HDR_FULL_FILE_NAME} \
        --host=${DATABASE_HOST}:${DATABASE_PORT} |
      tee $OUT_FULL_FILE_NAME

      # Retrieve command stats output
      redis-cli -h ${DATABASE_HOST} -p ${DATABASE_PORT} info commandstats >> $OUT_FULL_FILE_NAME
      redis-cli -h ${DATABASE_HOST} -p ${DATABASE_PORT} info commandstats

      echo "Sleeping for ${SLEEP_BETWEEN_RUNS} seconds"
      sleep ${SLEEP_BETWEEN_RUNS}
  done
  echo "Sleeping for ${SLEEP_BETWEEN_RUNS} seconds"
  sleep ${SLEEP_BETWEEN_RUNS}

done