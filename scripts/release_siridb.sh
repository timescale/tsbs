#!/bin/bash

NUMBER_OF_SERVERS=${NUMBER_OF_SERVERS:-2}
END=`expr $NUMBER_OF_SERVERS - 1`

# Create the directories for config file(s).
mkdir /tmp/siridb/

# Configuration of SiriDB
for i in $(seq 0 $END); do
# Create a directory for every server to store the database.
# And remove the old database if present.
rm /tmp/siridb/dbpath$i/ -r
mkdir /tmp/siridb/dbpath$i/
`cat <<EOT > /tmp/siridb/tsbs-siridb$i.conf
[siridb]
listen_client_port = 900$i
server_name = %HOSTNAME:901$i
ip_support = ALL
optimize_interval = 900
heartbeat_interval = 30
default_db_path = /tmp/siridb/dbpath$i
max_open_files = 512
enable_shard_compression = 1
enable_pipe_support = 0
buffer_sync_interval = 500
EOT`

SIRIDB_SERVER_DIR="siridb-server -l debug"
DB_DIR="/tmp/siridb/tsbs-siridb$i.conf"

xterm -e ${SIRIDB_SERVER_DIR} -c ${DB_DIR} &
done


