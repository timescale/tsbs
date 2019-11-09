#!/bin/bash

# Ensure generator is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_generate_data)}
if [[ -z "${EXE_FILE_NAME}" ]]; then
    echo "tsbs_generate_data not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Data folder
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_data"}

# Space-separated list of target DB formats to generate
FORMATS=${FORMATS:-"timescaledb"}

# Number of hosts to generate data about
SCALE=${SCALE:-"4000"}

# Rand seed
SEED=${SEED:-"123"}

# Start and stop time for generated timeseries
TS_START=${TS_START:-"2016-01-01T00:00:00Z"}
TS_END=${TS_END:-"2016-01-04T00:00:00Z"}

# What set of data to generate: devops (multiple data), cpu-only (cpu-usage data)
USE_CASE=${USE_CASE:-"cpu-only"}

# Step to generate data
LOG_INTERVAL=${LOG_INTERVAL:-"10s"}

# Max number of points to generate data. 0 means "use TS_START TS_END with LOG_INTERVAL"
MAX_DATA_POINTS=${MAX_DATA_POINTS:-"0"}

# Ensure DATA DIR available
mkdir -p ${BULK_DATA_DIR}
chmod a+rwx ${BULK_DATA_DIR}

pushd ${BULK_DATA_DIR}
set -eo pipefail

# Loop over all requested target formats and generate data
for FORMAT in ${FORMATS}; do
    DATA_FILE_NAME="data_${FORMAT}_${USE_CASE}_${SCALE}_${TS_START}_${TS_END}_${LOG_INTERVAL}_${SEED}.dat.gz"
    if [ -f "${DATA_FILE_NAME}" ]; then
        echo "WARNING: file ${DATA_FILE_NAME} already exists, skip generating new data"
    else
        cleanup() {
            rm -f ${DATA_FILE_NAME}
            exit 1
        }
        trap cleanup EXIT

        echo "Generating ${DATA_FILE_NAME}:"
        ${EXE_FILE_NAME} \
            --format ${FORMAT} \
            --use-case ${USE_CASE} \
            --scale ${SCALE} \
            --timestamp-start ${TS_START} \
            --timestamp-end ${TS_END} \
            --seed ${SEED} \
            --log-interval ${LOG_INTERVAL} \
            --max-data-points ${MAX_DATA_POINTS} \
        | gzip > ${DATA_FILE_NAME}

        trap - EXIT
        # Make short symlink for convenience
        SYMLINK_NAME="${FORMAT}-data.gz"

        rm -f ${SYMLINK_NAME} 2> /dev/null
        ln -s ${DATA_FILE_NAME} ${SYMLINK_NAME}

        # Make files readable by everyone
        chmod a+r ${DATA_FILE_NAME} ${SYMLINK_NAME}

        ls -lh ${SYMLINK_NAME}
    fi
done
