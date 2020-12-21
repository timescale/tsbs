#!/bin/bash
# showcases the ftsb 3 phases for timescaledb
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

MAX_RPS=${MAX_RPS:-"0"}
MAX_QUERIES=${MAX_QUERIES:-"1000"}
PASSWORD=${PASSWORD:-"password"}

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

# generate queries
$GOPATH/bin/tsbs_generate_data --format timescaledb --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/timescaledb_data

# generate queries
for QUERY_TYPE in ${QUERY_TYPES}; do
  $GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format timescaledb --use-case cpu-only --scale 10 --seed 123 --query-type ${QUERY_TYPE} --file /tmp/bulk_data/timescaledb_query_${QUERY_TYPE}
done
# insert benchmark
$GOPATH/bin/tsbs_load_timescaledb --pass=${PASSWORD} --postgres="sslmode=disable port=5433" --db-name=benchmark --host=127.0.0.1 --user=postgres --workers=1 --file=/tmp/bulk_data/timescaledb_data

# Loop over all requested queries types and generate data
for QUERY_TYPE in ${QUERY_TYPES}; do
  # queries benchmark
  $GOPATH/bin/tsbs_run_queries_timescaledb --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_timescaledb_query_${QUERY_TYPE}.hdr" --pass=${PASSWORD} --postgres="sslmode=disable port=5433" --db-name=benchmark --hosts=127.0.0.1 --user=postgres --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/timescaledb_query_${QUERY_TYPE}
done
