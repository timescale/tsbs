#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME_LOAD_DATA=${EXE_FILE_NAME_LOAD_DATA:-$(which tsbs_load_tdenginestmt2)}
if [[ -z "$EXE_FILE_NAME_LOAD_DATA" ]]; then
    echo "tsbs_load_tdenginestmt2 is not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-TDengineStmt2-data.gz}
DATABASE_NAME=${DATABASE_NAME:-benchmark}
DATABASE_HOST=${DATABASE_HOST:-localhost}
DATABASE_TAOS_PORT=${DATABASE_TAOS_PORT:-6030}
DATABASE_TAOS_PWD=${DATABASE_TAOS_PWD:-taosdata}

# Load parameters - personal
VGROUPS=${VGROUPS:-"2"}
BUFFER=${BUFFER:-"256"}
PAGES=${PAGES:-"256"}
TRIGGER=${TRIGGER:-"1"} 
WALFSYNCPERIOD=${WALFSYNCPERIOD:-"3000"}
WAL_LEVEL=${WAL_LEVEL:-"1"}
HASH_WORKERS=${HASH_WORKERS:-false}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME_LOAD_DATA \
                                --db-name=${DATABASE_NAME} \
                                --host=${DATABASE_HOST} \
                                --port=${DATABASE_TAOS_PORT} \
                                --pass=${DATABASE_TAOS_PWD} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --vgroups=${VGROUPS} \
                                --buffer=${BUFFER} \
                                --pages=${PAGES} \
                                --hash-workers=${HASH_WORKERS} \
                                --stt_trigger=${TRIGGER} \
                                --wal_level=${WAL_LEVEL} \
                                --wal_fsync_period=${WALFSYNCPERIOD} 
