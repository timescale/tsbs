#!/bin/bash
#
# QUERIES_FILE=/path/to/queries/file ./query.sh influxdb
#
# Overridable variables are mostly in common.sh and load_common.sh
#
# To run all sets of queries on all databases:
# $ ./query.sh
#
# To run all sets of queries on a specific database:
# $ ./query.sh influxdb
#
# To run a single set of queries on a specific database:
# $ QUERIES_FILE=/path/to/queries/file ./query.sh influxdb
#
set -e
#set -x

EXE_DIR=${EXE_DIR:-$(dirname $0)}
export EXE_DIR
source ${EXE_DIR}/common.sh
QUERIES_DATA_DIR=${QUERIES_DATA_DIR:-${DATA_DIR}/1/queries}


CONTAINER_STOP_SCRIPT=`cat <<'ENDSSH'
# remove all docker instances
CONTAINERS=$(docker ps -q -a)
if [ -n "${CONTAINERS}" ]; then
  docker rm -vf ${CONTAINERS}; 
fi
ENDSSH
`
echo "Stopping and removing running containers on host ${DATABASE_HOST}"

ssh ${DATABASE_HOST} "${CONTAINER_STOP_SCRIPT}"

for target in ${TEST_TARGETS}; do
    START_SCRIPT=${EXE_DIR}/"start_$target.sh"
    QUERY_SCRIPT=${EXE_DIR}/"query_$target.sh"
    
    # Run target database container
    echo "# Running database ${target}"
    ssh ${DATABASE_HOST} "DATA_DIR=${DATA_DIR} /bin/bash -s" < ${START_SCRIPT}
    
    TARGET_DATA=${BULK_DATA_DIR}/${target}

    # Let database start
    echo "Waiting 10s for ${target} to start"
    sleep 10

    if [ -z ${QUERIES_FILE} ]; then 
        for QUERIES_FILE in `ls -1 ${QUERIES_DATA_DIR}/${target}-*`; do
            echo "Querying ${target} using ${QUERIES_FILE}"
            source ${QUERY_SCRIPT}
        done
    else
        source ${QUERY_SCRIPT}
    fi
done

