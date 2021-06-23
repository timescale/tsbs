#!/bin/bash -x
# showcases the ftsb 3 phases for timescaledb
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

# Auth

PORT=5432
USER=postgres
PASSWORD=password
HOST=0.0.0.0
DATABASE_NAME=${DATABASE_NAME:-"benchmark"}
CONTAINER_NAME=timescaledb_benchmark
# setup pg with password and expose the default port.
sudo docker run -d --name $CONTAINER_NAME -p 5432:$PORT \
  -e POSTGRES_PASSWORD=$PASSWORD \
  timescale/timescaledb:latest-pg12

docker start $CONTAINER_NAME
sleep 2


# Setup
TARGET=timescaledb
USE_CASE=${USE_CASE:-"cpu-only"}
QUERY_TYPES=(lastpoint cpu-max-all-1 high-cpu-1)
SCALE=10
SEED=123
WORKERS=1
MAX_QUERIES=${MAX_QUERIES:-"1000"}
LOG_INTERVAL="3600s"
START_TIME="2021-01-01T00:00:00Z"
END_TIME="2021-01-07T00:00:00Z"

# 7 days with 10/sec = ~6 Milion records

# Folders setup

TARGET_DATA_FOLDER=${TARGET_DATA_FOLDER:-"/tmp/bulk_data"}
DATA_FILE="${TARGET_DATA_FOLDER}/${TARGET}_data_${USE_CASE}_${SCALE}"
mkdir -p $TARGET_DATA_FOLDER


# generate queries
$GOPATH/bin/tsbs_generate_data \
    --format $TARGET \
    --use-case $USE_CASE \
    --scale $SCALE \
    --log-interval $LOG_INTERVAL \
    --timestamp-start $START_TIME \
    --timestamp-end  $END_TIME \
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
    --timestamp-start $START_TIME \
    --timestamp-end  $END_TIME \
    --query-type $_type \
    --file $TARGET_DATA_FOLDER/timescaledb_query_${_type}_${USE_CASE}_${SCALE}
     # FIXME: add to filename with the proper format _${START_TIME}_${END_TIME}
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

# only because you're going to do benchmarks and don't wanto to persist it
sudo docker rm -f $(sudo docker ps -aq --filter name=$CONTAINER_NAME)
