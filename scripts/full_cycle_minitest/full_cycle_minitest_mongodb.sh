#!/bin/bash
# showcases the ftsb 3 phases for MongoDB
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

MAX_RPS=${MAX_RPS:-"0"}
MAX_QUERIES=${MAX_QUERIES:-"1000"}

mkdir -p /tmp/bulk_data

# generate data
$GOPATH/bin/tsbs_generate_data --format mongo --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/mongo_data

# generate queries
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format mongo --use-case cpu-only --scale 10 --seed 123 --query-type lastpoint --file /tmp/bulk_data/mongo_query_lastpoint
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format mongo --use-case cpu-only --scale 10 --seed 123 --query-type cpu-max-all-1 --file /tmp/bulk_data/mongo_query_cpu-max-all-1
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format mongo --use-case cpu-only --scale 10 --seed 123 --query-type high-cpu-1 --file /tmp/bulk_data/mongo_query_high-cpu-1
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format mongo --use-case cpu-only --scale 10 --seed 123 --query-type single-groupby-1-1-1 --file /tmp/bulk_data/mongo_query_single-groupby-1-1-1
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format mongo --use-case cpu-only --scale 10 --seed 123 --query-type groupby-orderby-limit --file /tmp/bulk_data/mongo_query_groupby-orderby-limit

# insert benchmark
$GOPATH/bin/tsbs_load_mongo --db-name=benchmark --batch-size=100 --workers=1 --document-per-event=true --timeseries-collection=true --retryable-writes=false --random-field-order=false --file=/tmp/bulk_data/mongo_data --results-file="mongo_load_results.json"

# queries benchmark
$GOPATH/bin/tsbs_run_queries_mongo --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_mongo_query_lastpoint.hdr" --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/mongo_query_lastpoint --results-file="mongo_query_lastpoint_results.json"
$GOPATH/bin/tsbs_run_queries_mongo --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_mongo_query_cpu-max-all-1.hdr" --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/mongo_query_cpu-max-all-1  --results-file="mongo_query_cpu-max-all-1_results.json"
$GOPATH/bin/tsbs_run_queries_mongo --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_mongo_query_high-cpu-1.hdr" --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/mongo_query_high-cpu-1 --results-file="mongo_query_high-cpu-1_results.json"
$GOPATH/bin/tsbs_run_queries_mongo --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_mongo_query_single-groupby-1-1-1.hdr" --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/mongo_query_single-groupby-1-1-1 --results-file="mongo_query_single-groupby-1-1-1_results.json"
$GOPATH/bin/tsbs_run_queries_mongo --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_mongo_query_groupby-orderby-limit.hdr" --db-name=benchmark --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/mongo_query_groupby-orderby-limit --results-file="mongo_query_groupby-orderby-limit_results.json"
