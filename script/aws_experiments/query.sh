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
RESULTS_DIR=${QUERIES_OUTPUT_DIR:-${QUERIES_DATA_DIR}/results}

CONTAINER_STOP_SCRIPT=`cat <<'ENDSSH'
# remove all docker instances
CONTAINERS=$(docker ps -q -a)
if [ -n "${CONTAINERS}" ]; then
  docker stop ${CONTAINERS};
  docker rm -vf ${CONTAINERS}; 
fi
ENDSSH
`

mkdir -p ${RESULTS_DIR}
chmod a+rwx ${RESULTS_DIR}

for target in ${TEST_TARGETS}; do
    START_SCRIPT=${EXE_DIR}/"start_$target.sh"
    QUERY_SCRIPT=${EXE_DIR}/"query_$target.sh"

    echo "Stopping and removing running containers on host ${DATABASE_HOST}"
    ssh ${DATABASE_HOST} "${CONTAINER_STOP_SCRIPT}"
    
    # Run target database container
    echo "# Running database ${target}"
    ssh ${DATABASE_HOST} "DATA_DIR=${DATA_DIR} /bin/bash -s" < ${START_SCRIPT}
    
    # Let database start
    #echo "Waiting 10s for ${target} to start"
    #sleep 10   

    if [ -z ${QUERIES_FILE} ]; then
	# Remove "db" ending from influxdb target. This is a hack needed because the 
	# with influxdb the target does not match the query format strings
	FORMAT=`echo ${target} | sed s/influxdb/influx/`

        for QUERIES_FILE in `ls -1 ${QUERIES_DATA_DIR}/${FORMAT}-*`; do
	    export QUERIES_FILE
            QUERIES_FILE_BASE=$(basename ${QUERIES_FILE})
	    RESULTS_FILE=${RESULTS_DIR}/${QUERIES_FILE_BASE}.results
	    echo "Querying ${target} using ${QUERIES_FILE} and writing results to ${RESULTS_FILE}"
            ${QUERY_SCRIPT} 2>&1 | tee ${RESULTS_FILE}
        done
    else
	export DATA_DIR QUERIES_DATA_DIR QUERIES_FILE PRINT_RESPONSES
        QUERIES_FILE_BASE=$(basename ${QUERIES_FILE})
        ${QUERY_SCRIPT} 2>&1 | tee ${RESULTS_DIR}/${QUERIES_FILE_BASE}.results
    fi
done

