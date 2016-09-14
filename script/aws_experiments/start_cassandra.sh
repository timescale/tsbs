#!/bin/bash
set -x
DATA_DIR=${DATA_DIR:-/disk}
CASSANDRA_VERSION=${CASSANDRA_VERSION:-3.0.7}
CASSANDRA_CONF=${CASSANDRA_CONF:-/tmp/cassandra.conf}
CASSANDRA_DATA_DIR=${CASSANDRA_DATA_DIR:-${DATA_DIR}/1/cassandra/data}
CASSANDRA_WAL_DIR=${CASSANDRA_WAL_DIR:-${DATA_DIR}/2/cassandra/wal}

docker stop cassandra &> /dev/null

# Generate default config
echo "Data dirs: ${DATA_DIR} ${CASSANDRA_DATA_DIR}"
docker run -d \
       --name cassandra \
       -p 7000:7000 \
       -p 7001:7001 \
       -p 7199:7199 \
       -p 9042:9042 \
       -v ${CASSANDRA_DATA_DIR}:/var/lib/cassandra \
       -v ${CASSANDRA_WAL_DIR}:/var/lib/cassandra/commitlog \
       cassandra:${CASSANDRA_VERSION}