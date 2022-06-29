#!/bin/bash

# Database credentials
DATABASE_HOST=${DATABASE_HOST:-"localhost"}
DATABASE_NAME=${DATABASE_NAME:-"benchmark"}

# Data folder
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_queries"}

# Data folder
RESULTS_DIR=${RESULTS_DIR:-"./results"}

# Load parameters
BATCH_SIZE=${BATCH_SIZE:-10000}
# Debug
DEBUG=${DEBUG:-0}
# How many concurrent worker would load data - match num of cores, or default to 8
NUM_WORKERS=${NUM_WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}
BACKOFF_SECS=${BACKOFF_SECS:-1s}
REPORTING_PERIOD=${REPORTING_PERIOD:-1s}
#
#set -x
