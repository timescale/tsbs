#!/bin/bash

# Fail immediately
set -e
# showcases the ftsb 3 phases for redistimeseries
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

# Database credentials
REDISTIMESERIES_HOST=${REDISTIMESERIES_HOST:-"localhost"}
REDISTIMESERIES_PORT=${REDISTIMESERIES_PORT:-6379}
REDISTIMESERIES_URI="$REDISTIMESERIES_HOST:$REDISTIMESERIES_PORT"
CLUSTER_FLAG=${CLUSTER_FLAG:-""}

# generate data
mkdir -p /tmp/bulk_data
./bin/tsbs_generate_data --format redistimeseries --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/redistimeseries_data

# generate queries
./bin/tsbs_generate_queries --format redistimeseries --use-case cpu-only --scale 10 --seed 123 --query-type lastpoint             --file /tmp/bulk_data/redistimeseries_query_lastpoint
./bin/tsbs_generate_queries --format redistimeseries --use-case cpu-only --scale 10 --seed 123 --query-type cpu-max-all-1         --file /tmp/bulk_data/redistimeseries_query_cpu-max-all-1
./bin/tsbs_generate_queries --format redistimeseries --use-case cpu-only --scale 10 --seed 123 --query-type high-cpu-1            --file /tmp/bulk_data/redistimeseries_query_high-cpu-1
./bin/tsbs_generate_queries --format redistimeseries --use-case cpu-only --scale 10 --seed 123 --query-type single-groupby-5-1-1  --file /tmp/bulk_data/redistimeseries_query_single-groupby-5-1-1
./bin/tsbs_generate_queries --format redistimeseries --use-case cpu-only --scale 10 --seed 123 --query-type groupby-orderby-limit --file /tmp/bulk_data/redistimeseries_query_groupby-orderby-limit

# insert benchmark
./bin/tsbs_load_redistimeseries  --host=$REDISTIMESERIES_URI $CLUSTER_FLAG --workers=1 --file=/tmp/bulk_data/redistimeseries_data --results-file="redistimeseries_load_results.json"

# queries benchmark
./bin/tsbs_run_queries_redistimeseries  --host=$REDISTIMESERIES_URI $CLUSTER_FLAG --workers=1 --max-queries=10 --file=/tmp/bulk_data/redistimeseries_query_lastpoint
./bin/tsbs_run_queries_redistimeseries  --host=$REDISTIMESERIES_URI $CLUSTER_FLAG --workers=1 --max-queries=10 --file=/tmp/bulk_data/redistimeseries_query_cpu-max-all-1
./bin/tsbs_run_queries_redistimeseries  --host=$REDISTIMESERIES_URI $CLUSTER_FLAG --workers=1 --max-queries=10 --file=/tmp/bulk_data/redistimeseries_query_high-cpu-1
./bin/tsbs_run_queries_redistimeseries  --host=$REDISTIMESERIES_URI $CLUSTER_FLAG --workers=1 --max-queries=10 --file=/tmp/bulk_data/redistimeseries_query_single-groupby-5-1-1
./bin/tsbs_run_queries_redistimeseries  --host=$REDISTIMESERIES_URI $CLUSTER_FLAG --workers=1 --max-queries=10 --file=/tmp/bulk_data/redistimeseries_query_groupby-orderby-limit
