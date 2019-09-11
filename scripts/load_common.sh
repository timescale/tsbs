#!/bin/bash

# Database credentials
DATABASE_HOST=${DATABASE_HOST:-"localhost"}
DATABASE_NAME=${DATABASE_NAME:-"benchmark"}
PORT=${PORT:-5432}

# Data folder
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_data"}
# Full path to data file
DATA_FILE=${DATA_FILE:-${BULK_DATA_DIR}/${DATA_FILE_NAME}}

# Load parameters
BATCH_SIZE=${BATCH_SIZE:-10000}
# How many concurrent worker would load data - match num of cores, or default to 4
NUM_WORKERS=${NUM_WORKERS:-$(grep -c ^processor /proc/cpuinfo 2> /dev/null || echo 4)}
BACKOFF_SECS=${BACKOFF_SECS:-1s}
REPORTING_PERIOD=${REPORTING_PERIOD:-10s}

DO_CREATE_DB=${DO_CREATE_DB:-true}

# Ensure data file is in place
if [ ! -f ${DATA_FILE} ]; then
   echo "Cannot find data file ${DATA_FILE}"
   exit -1
fi

echo "Bulk loading file ${DATA_FILE}"

set -x
