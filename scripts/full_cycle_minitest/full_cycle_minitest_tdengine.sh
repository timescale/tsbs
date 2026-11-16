#!/bin/bash
# showcases the ftsb 3 phases for TDengine
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

MAX_QUERIES=${MAX_QUERIES:-"1000"}

mkdir -p /tmp/bulk_data

# generate data
$GOPATH/bin/tsbs_generate_data --format TDengine --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/tdengine_data

# generate queries
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format TDengine --use-case cpu-only --scale 10 --seed 123 --query-type lastpoint     --file /tmp/bulk_data/tdengine_query_lastpoint
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format TDengine --use-case cpu-only --scale 10 --seed 123 --query-type cpu-max-all-1 --file /tmp/bulk_data/tdengine_query_cpu-max-all-1
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format TDengine --use-case cpu-only --scale 10 --seed 123 --query-type high-cpu-1    --file /tmp/bulk_data/tdengine_query_high-cpu-1

# insert benchmark
$GOPATH/bin/tsbs_load_tdengine --db-name=benchmark  --workers=1 --file=/tmp/bulk_data/tdengine_data --results-file="tdengine_load_results.json"

# queries benchmark
$GOPATH/bin/tsbs_run_queries_tdengine --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/tdengine_query_lastpoint --results-file="tdengine_query_lastpoint_results.json"
$GOPATH/bin/tsbs_run_queries_tdengine --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/tdengine_query_cpu-max-all-1  --results-file="tdengine_query_cpu-max-all-1_results.json"
$GOPATH/bin/tsbs_run_queries_tdengine --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/tdengine_query_high-cpu-1 --results-file="tdengine_query_high-cpu-1_results.json"