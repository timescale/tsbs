#!/bin/bash

# Database credentials
DATABASE_HOST=${DATABASE_HOST:-"localhost"}
DATABASE_PORT=${DATABASE_PORT:-6379}
PIPELINE=${PIPELINE:-100}
CONNECTIONS=${CONNECTIONS:-50}

# Load parameters
BATCH_SIZE=${BATCH_SIZE:-10000}
# Debug
DEBUG=${DEBUG:-0}

SCALE=${SCALE:-"100"}
CLUSTER_FLAG=${CLUSTER_FLAG:-""}

# How many concurrent worker would load data - match num of cores, or default to 8
NUM_WORKERS=${NUM_WORKERS:-$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 8)}
REPORTING_PERIOD=${REPORTING_PERIOD:-1s}

REPETITIONS=${REPETITIONS:-3}

# Rand seed
SEED=${SEED:-"123"}

# Print timing stats to stderr after this many queries (0 to disable)
QUERIES_PRINT_INTERVAL=${QUERIES_PRINT_INTERVAL:-"1000"}

# How many queries would be run
MAX_QUERIES=${MAX_QUERIES:-"0"}
REPETITIONS=${REPETITIONS:-1}
PREFIX=${PREFIX:-""}

# How many queries would be run
SLEEP_BETWEEN_RUNS=${SLEEP_BETWEEN_RUNS:-"0"}

# What set of data to generate: devops (multiple data), cpu-only (cpu-usage data)
USE_CASE=${USE_CASE:-"cpu-only"}

##########################
# Data generation related
# For benchmarking read latency, we used the following setup for each database (the machine configuration is the same as the one used in the Insert comparison):
#    Dataset: 100–4,000 simulated devices generated 1–10 CPU metrics every 10 seconds for 4 full days (100M+ reading intervals, 1B+ metrics)
#    10,000 batch size should be used

# Start and stop time for generated timeseries
TS_START=${TS_START:-"2016-01-01T00:00:00Z"}
TS_END=${TS_END:-"2016-01-04T00:00:00Z"}

LOG_INTERVAL=${LOG_INTERVAL:-"10s"}

# Max number of points to generate data. 0 means "use TS_START TS_END with LOG_INTERVAL"
MAX_DATA_POINTS=${MAX_DATA_POINTS:-"0"}

FORMAT=${FORMAT:-"redistimeseries"}
INTERLEAVED_GENERATION_GROUPS=${INTERLEAVED_GENERATION_GROUPS:-"1"}
IG_GROUPS=${IG_GROUPS:-"1"}
IG_GROUPS_ID=${IG_GROUPS_ID:-"0"}
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_data_${FORMAT}"}
DATA_FILE_NAME="${BULK_DATA_DIR}/data_${FORMAT}_${USE_CASE}_${SCALE}_${TS_START}_${TS_END}_${LOG_INTERVAL}_${SEED}.dat"

# Results folder
RESULTS_DIR=${RESULTS_DIR:-"./results"}
