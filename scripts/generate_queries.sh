#!/bin/bash

# Ensure generator is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_generate_queries)}
if [[ -z "${EXE_FILE_NAME}" ]]; then
    echo "tsbs_generate_queries not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

EXE_FILE_VERSION=`md5sum $EXE_FILE_NAME | awk '{ print $1 }'`
# Queries folder
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_queries"}

# Form of data to generate
USE_JSON=${USE_JSON:-false}
USE_TAGS=${USE_TAGS:-true}
USE_TIME_BUCKET=${USE_TIME_BUCKET:-true}

# Space-separated list of target DB formats to generate
FORMATS=${FORMATS:-"timescaledb"}

# All available for generation query types (sorted alphabetically)
QUERY_TYPES_ALL="\
cpu-max-all-1 \
cpu-max-all-8 \
double-groupby-1 \
double-groupby-5 \
double-groupby-all \
groupby-orderby-limit \
high-cpu-1 \
high-cpu-all \
lastpoint \
single-groupby-1-1-1 \
single-groupby-1-1-12 \
single-groupby-1-8-1 \
single-groupby-5-1-1 \
single-groupby-5-1-12 \
single-groupby-5-8-1"

# What query types to generate
QUERY_TYPES=${QUERY_TYPES:-$QUERY_TYPES_ALL}

# Number of hosts to generate data about
SCALE=${SCALE:-"4000"}

# Number of queries to generate
QUERIES=${QUERIES:-"1000"}

# Rand seed
SEED=${SEED:-"123"}

# Start and stop time for generated timeseries
TS_START=${TS_START:-"2016-01-01T00:00:00Z"}
TS_END=${TS_END:-"2016-01-04T00:00:01Z"}

# What set of data to generate: devops (multiple data), cpu-only (cpu-usage data)
USE_CASE=${USE_CASE:-"cpu-only"}

# Ensure DATA DIR available
mkdir -p ${BULK_DATA_DIR}
chmod a+rwx ${BULK_DATA_DIR}

pushd ${BULK_DATA_DIR}
set -eo pipefail

# Loop over all requested queries types and generate data
for QUERY_TYPE in ${QUERY_TYPES}; do
    for FORMAT in ${FORMATS}; do
        DATA_FILE_NAME="queries_${FORMAT}_${QUERY_TYPE}_${EXE_FILE_VERSION}_${QUERIES}_${SCALE}_${SEED}_${TS_START}_${TS_END}_${USE_CASE}.dat.gz"
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
                --queries ${QUERIES} \
                --query-type ${QUERY_TYPE} \
                --scale ${SCALE} \
                --seed ${SEED} \
                --timestamp-start ${TS_START} \
                --timestamp-end ${TS_END} \
                --use-case ${USE_CASE} \
                --timescale-use-json=${USE_JSON} \
                --timescale-use-tags=${USE_TAGS} \
                --timescale-use-time-bucket=${USE_TIME_BUCKET} \
                --clickhouse-use-tags=${USE_TAGS} \
            | gzip  > ${DATA_FILE_NAME}

            trap - EXIT
            # Make short symlink for convenience
            SYMLINK_NAME="${FORMAT}-${QUERY_TYPE}-queries.gz"

            rm -f ${SYMLINK_NAME} 2> /dev/null
            ln -s ${DATA_FILE_NAME} ${SYMLINK_NAME}

            # Make files accessible by everyone
            chmod a+r ${DATA_FILE_NAME} ${SYMLINK_NAME}

            ls -lh ${SYMLINK_NAME}
        fi
    done
done
