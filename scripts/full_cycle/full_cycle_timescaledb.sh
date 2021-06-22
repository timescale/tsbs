#!/bin/bash -x
# showcases the ftsb 3 phases for timescaledb
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

# Auth
HOST=127.0.0.1
PORT=5432
USER=postgres
PASSWORD=${PASSWORD:-""}
DATABASE_NAME=${DATABASE_NAME:-"benchmark"}

# Setup
USE_CASE=${USE_CASE:-"cpu-only"}
QUERY_TYPES=(lastpoint cpu-max-all-1 high-cpu-1)
SCALE=10
SEED=123
WORKERS=1
MAX_QUERIES=${MAX_QUERIES:-"1000"}

# Folders setup

TARGET_DATA_FOLDER=${TARGET_DATA_FOLDER:-"/tmp/bulk_data"}
DATA_FILE=$TARGET_DATA_FOLDER/timescaledb_data
mkdir -p $TARGET_DATA_FOLDER


# generate queries
$GOPATH/bin/tsbs_generate_data \
    --format timescaledb \
    --use-case $USE_CASE \
    --scale $SCALE \
    --seed $SEED \
    --file $DATA_FILE

for _type in "${QUERY_TYPES[@]}"
do
   :
  $GOPATH/bin/tsbs_generate_queries \
    --format timescaledb \
    --queries ${MAX_QUERIES} \
    --use-case $USE_CASE \
    --scale $SCALE \
    --seed $SEED \
    --query-type $_type \
    --file $TARGET_DATA_FOLDER/timescaledb_query_$_type
done

# insert benchmark
$GOPATH/bin/tsbs_load_timescaledb \
    --pass=${PASSWORD} \
    --postgres="sslmode=disable port=${PORT}" \
    --db-name=${DATABASE_NAME} \
    --host=${HOST} \
    --user=${USER} \
    --workers=${WORKERS} \
    --file=${DATA_FILE}


for _type in "${QUERY_TYPES[@]}"
do
   :
    # queries benchmark
    $GOPATH/bin/tsbs_run_queries_timescaledb \
        --max-queries=${MAX_QUERIES} \
        --pass=${PASSWORD} \
        --postgres="sslmode=disable port=${PORT}" \
        --db-name=${DATABASE_NAME} \
        --hosts=${HOST} \
        --user=${USER} \
        --workers=${WORKERS} \
        --file=$TARGET_DATA_FOLDER/timescaledb_query_$_type
done
