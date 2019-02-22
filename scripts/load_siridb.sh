#!/bin/bash

# Create the directories for the database and config file.
# And remove the old database if present.
mkdir /tmp/siridb/
rm /tmp/siridb/dbpath/ -r
mkdir /tmp/siridb/dbpath/

# Configuration of SiriDB
# NOTE: only 1 SiriDB server can be started with this shell script.
cat <<EOT > /tmp/siridb/tsbs-siridb.conf
[siridb]
listen_client_port = 9000
server_name = %HOSTNAME:9010
ip_support = ALL
optimize_interval = 9
heartbeat_interval = 30
default_db_path = /tmp/siridb/dbpath
max_open_files = 512
enable_shard_compression = 1
enable_pipe_support = 0
buffer_sync_interval = 500
EOT

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_siridb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_siridb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-siridb-data.gz}

# Load parameters - personal
DATABASE_USER=${DATABASE_USER:-iris}
DATABASE_PASS=${DATABASE_PASS:-siri}
DATABASE_PORT=${DATABASE_PORT:-9000}
SIRIDB_SERVER_DIR=${SIRIDB_SERVER_DIR:-"siridb-server -l debug"}
DB_DIR=${DB_DIR:-"/tmp/siridb/tsbs-siridb.conf"}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

until nc -z ${DATABASE_HOST} ${DATABASE_PORT}; do
    xterm -e ${SIRIDB_SERVER_DIR} -c ${DB_DIR} &
    echo "Waiting for SiriDB"
    sleep 1
done

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --hosts=${DATABASE_HOST}:${DATABASE_PORT} \
                                --dbuser=${DATABASE_USER} \
                                --dbpass=${DATABASE_PASS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --do-load=true \
                                --log-batches=false \



