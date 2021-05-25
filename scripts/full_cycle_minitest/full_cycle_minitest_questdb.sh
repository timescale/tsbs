#!/usr/bin/env bash
# showcases the ftsb 3 phases for questdb
# - 1) data generation
# - 2) data loading/insertion
# - 3) query execution

# generate data
mkdir -p /tmp/bulk_data
$GOPATH/bin/tsbs_generate_data --format questdb --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/questdb_data

# generate queries
$GOPATH/bin/tsbs_generate_queries --format questdb --use-case cpu-only --scale 10 --seed 123 --query-type lastpoint --file /tmp/bulk_data/questdb_query_lastpoint
$GOPATH/bin/tsbs_generate_queries --format questdb --use-case cpu-only --scale 10 --seed 123 --query-type cpu-max-all-1 --file /tmp/bulk_data/questdb_query_cpu-max-all-1
$GOPATH/bin/tsbs_generate_queries --format questdb --use-case cpu-only --scale 10 --seed 123 --query-type high-cpu-1 --file /tmp/bulk_data/questdb_query_high-cpu-1
$GOPATH/bin/tsbs_generate_queries --format questdb --use-case cpu-only --scale 10 --seed 123 --query-type single-groupby-5-1-1 --file /tmp/bulk_data/questdb_query_single-groupby-5-1-1
$GOPATH/bin/tsbs_generate_queries --format questdb --use-case cpu-only --scale 10 --seed 123 --query-type groupby-orderby-limit --file /tmp/bulk_data/questdb_query_groupby-orderby-limit

# insert benchmark
$GOPATH/bin/tsbs_load_questdb --file=/tmp/bulk_data/questdb_data

# queries benchmark
$GOPATH/bin/tsbs_run_queries_questdb --max-queries=10 --file=/tmp/bulk_data/questdb_query_lastpoint
$GOPATH/bin/tsbs_run_queries_questdb --max-queries=10 --file=/tmp/bulk_data/questdb_query_cpu-max-all-1
$GOPATH/bin/tsbs_run_queries_questdb --max-queries=10 --file=/tmp/bulk_data/questdb_query_high-cpu-1
$GOPATH/bin/tsbs_run_queries_questdb --max-queries=10 --file=/tmp/bulk_data/questdb_query_single-groupby-5-1-1
$GOPATH/bin/tsbs_run_queries_questdb --max-queries=10 --file=/tmp/bulk_data/questdb_query_groupby-orderby-limit
