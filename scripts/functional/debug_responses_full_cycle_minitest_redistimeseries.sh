#!/bin/bash
# showcases the ftsb 3 phases for timescaledb
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

SCALE=${SCALE:-"10"}
SEED=${SEED:-"123"}
PASSWORD=${PASSWORD:-"password"}
FORMAT="redistimeseries"

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

mkdir -p /tmp/bulk_data
rm /tmp/bulk_data/${FORMAT}_*
rm docs/responses/${FORMAT}_*

redis-cli flushall

# generate data
$GOPATH/bin/tsbs_generate_data --format ${FORMAT} --use-case cpu-only --scale=${SCALE} --seed=${SEED} --file /tmp/bulk_data/${FORMAT}_data

for queryName in $QUERY_TYPES; do
  $GOPATH/bin/tsbs_generate_queries --format ${FORMAT} --use-case cpu-only --scale=${SCALE} --seed=${SEED} \
    --queries=1 \
    --query-type $queryName \
    --file /tmp/bulk_data/${FORMAT}_query_$queryName
done

# insert benchmark
$GOPATH/bin/tsbs_load_${FORMAT} \
  --workers=1 \
  --file=/tmp/bulk_data/${FORMAT}_data

# queries benchmark
for queryName in $QUERY_TYPES; do
  echo "running query: $queryName"
  $GOPATH/bin/tsbs_run_queries_${FORMAT} --print-responses \
    --workers=1 \
    --debug=3 \
    --file /tmp/bulk_data/${FORMAT}_query_$queryName >docs/responses/${FORMAT}_$queryName.json
done
