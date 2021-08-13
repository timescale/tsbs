#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_victoriametrics)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_victoriametrics not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-victoriametrics-data.gz}
DATABASE_PORT=${DATABASE_PORT:-8428}
DATABASE_PATH=${DATABASE_PATH:write}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

# Load data
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --urls=http://${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_PATH}
