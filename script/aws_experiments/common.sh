#!/bin/bash
set -e
#set -x

BENCHMARK_DIR=${BENCHMARK_DIR:-${EXE_DIR}}
DATABASE_HOST=${DATABASE_HOST:-database}
DATA_DIR=${DATA_DIR:-/disk}
TEST_TARGETS=${@:-"influxdb iobeamdb cassandra"}

echo "# Test targets are: ${TEST_TARGETS}"

# First check that the given targets exist
for target in ${TEST_TARGETS}; do
    START_SCRIPT=${EXE_DIR}/"start_$target.sh"

    if [ ! -f ${START_SCRIPT} ]; then
        echo "$target is not a valid test target"
        exit
    fi
done

echo "# Database host is ${DATABASE_HOST}"
