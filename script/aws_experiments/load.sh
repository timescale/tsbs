#!/bin/bash

# For running locally, one can try:
# DATA_DIR=/tmp BULK_DATA_DIR=/tmp/datafiles ./load.sh influxdb
#
# This assumes that data has been generated in /tmp/datafiles
# with the utils/generate_data.sh script.
#
# Overridable variables are mostly in common.sh and load_common.sh
#
set -e
#set -x

EXE_DIR=${EXE_DIR:-$(dirname $0)}
export EXE_DIR
source ${EXE_DIR}/common.sh

CONTAINER_CLEANUP_SCRIPT=`cat <<'ENDSSH'
# remove all docker instances
CONTAINERS=$(docker ps -a -q)
if [ -n "${CONTAINERS}" ]; then
  docker rm -vf ${CONTAINERS}; 
fi
ENDSSH
`
ssh ${DATABASE_HOST} "${CONTAINER_CLEANUP_SCRIPT}"

for target in ${TEST_TARGETS}; do
    START_SCRIPT=${EXE_DIR}/"start_$target.sh"
    LOAD_SCRIPT=${EXE_DIR}/"load_$target.sh"

    # Cleanup data   
    ssh -t ${DATABASE_HOST} "sudo rm -rf ${DATA_DIR}/1/${target} ${DATA_DIR}/2/${target}"

    # Run target database container
    echo "# Running database ${target}"
    ssh ${DATABASE_HOST} "DATA_DIR=${DATA_DIR} /bin/bash -s" < ${START_SCRIPT}
    
    TARGET_DATA=${BULK_DATA_DIR}/${target}

    # Let database start
    echo "Waiting for ${target} to start"
    sleep 3
    
    echo "Loading ${target}"
    eval ${LOAD_SCRIPT}
done

