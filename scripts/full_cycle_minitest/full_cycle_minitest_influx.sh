#!/bin/bash
# showcases the ftsb 3 phases for influxdb
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

SCALE=${SCALE:-"10"}
SEED=${SEED:-"123"}
FORMAT="influx"

mkdir -p /tmp/bulk_data
rm /tmp/bulk_data/${FORMAT}_*

# exit immediately on error
set -e

# Load parameters - common
DATABASE_PORT=${DATABASE_PORT:-8086}
DATABASE_HOST=${DATABASE_HOST:-localhost}

# All available query types (sorted alphabetically)
QUERY_TYPES_ALL="\
  cpu-max-all-1 \
  cpu-max-all-8 \
  double-groupby-1 \
  double-groupby-5 \
  double-groupby-all \
  groupby-orderby-limit \
  high-cpu-1 \
  high-cpu-all \
  lastpoint \
  single-groupby-1-1-1 \
  single-groupby-1-1-12 \
  single-groupby-1-8-1 \
  single-groupby-5-1-1 \
  single-groupby-5-1-12 \
  single-groupby-5-8-1"

# What query types to generate
QUERY_TYPES=${QUERY_TYPES:-$QUERY_TYPES_ALL}

# generate data
$GOPATH/bin/tsbs_generate_data --format ${FORMAT} --use-case cpu-only --scale=${SCALE} --seed=${SEED} --file /tmp/bulk_data/${FORMAT}_data

for queryName in $QUERY_TYPES; do
  echo "generating query: $queryName"
  $GOPATH/bin/tsbs_generate_queries --format ${FORMAT} --use-case cpu-only --scale=${SCALE} --seed=${SEED} \
    --queries=10 \
    --query-type $queryName \
    --file /tmp/bulk_data/${FORMAT}_query_$queryName
done

until curl http://${DATABASE_HOST}:${DATABASE_PORT}/ping 2>/dev/null; do
  echo "Waiting for InfluxDB"
  sleep 1
done

# Remove previous database
curl -X POST http://${DATABASE_HOST}:${DATABASE_PORT}/query?q=drop%20database%20benchmark

# insert benchmark
$GOPATH/bin/tsbs_load_${FORMAT} \
  --db-name=benchmark \
  --backoff=1s \
  --workers=1 \
  --urls=http://${DATABASE_HOST}:${DATABASE_PORT} \
  --auth-token ${INFLUX_AUTH_TOKEN} \
  --file=/tmp/bulk_data/${FORMAT}_data

# queries benchmark
for queryName in $QUERY_TYPES; do
  echo "running query: $queryName"
  $GOPATH/bin/tsbs_run_queries_${FORMAT} --print-responses \
    --workers=1 \
    --auth-token ${INFLUX_AUTH_TOKEN} \
    --file /tmp/bulk_data/${FORMAT}_query_$queryName
done
