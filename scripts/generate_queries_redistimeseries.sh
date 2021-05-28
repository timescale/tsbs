#!/bin/bash

# Ensure generator is available
EXE_FILE_NAME=./bin/tsbs_generate_queries

set -x

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/query_common.sh
source ${EXE_DIR}/redistimeseries_common.sh
#  redistimeseries supported query types (sorted alphabetically)
QUERY_TYPES_ALL="\
cpu-max-all-1 \
cpu-max-all-8 \
double-groupby-1 \
double-groupby-5 \
double-groupby-all \
groupby-orderby-limit \
lastpoint \
single-groupby-1-1-1 \
single-groupby-1-1-12 \
single-groupby-1-8-1 \
single-groupby-5-1-1 \
single-groupby-5-1-12 \
single-groupby-5-8-1"
#high-cpu-1 \
#high-cpu-all \

# What query types to generate
QUERY_TYPES=${QUERY_TYPES:-$QUERY_TYPES_ALL}

# Number of queries to generate
QUERIES=${QUERIES:-"10000"}

# Whether to skip data generation if it already exists
SKIP_IF_EXISTS=${SKIP_IF_EXISTS:-"TRUE"}

# Ensure DATA DIR available
mkdir -p ${BULK_DATA_DIR}

# Ensure queries dir is clean
rm -rf ${BULK_DATA_DIR}/queries_${USE_CASE}_${FORMAT}_${SCALE}*

# Loop over all requested queries types and generate data
for QUERY_TYPE in ${QUERY_TYPES}; do
    QUERY_DATA_FILE_NAME="queries_${USE_CASE}_${FORMAT}_${SCALE}_${QUERY_TYPE}_${QUERIES}_${SEED}_${TS_START}_${TS_END}.dat"
    echo "Generating ${QUERY_DATA_FILE_NAME}:"
    ${EXE_FILE_NAME} \
        --format=${FORMAT} \
        --queries=${QUERIES} \
        --query-type=${QUERY_TYPE} \
        --scale=${SCALE} \
        --seed=${SEED} \
        --timestamp-start=${TS_START} \
        --timestamp-end=${TS_END} \
        --use-case=${USE_CASE} \
        > ${BULK_DATA_DIR}/${QUERY_DATA_FILE_NAME}

done
