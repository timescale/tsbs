#!/bin/bash
# showcases the ftsb 3 phases for cratedb
# - 1) data and query generation
# - 2) data loading/insertion
# - 3) query execution

# generate data
mkdir -p /tmp/bulk_data
$GOPATH/bin/tsbs_generate_data --format cratedb --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/cratedb_data

# generate queries
$GOPATH/bin/tsbs_generate_queries --format cratedb --use-case cpu-only --scale 10 --seed 123 --query-type lastpoint             --file /tmp/bulk_data/cratedb_query_lastpoint
$GOPATH/bin/tsbs_generate_queries --format cratedb --use-case cpu-only --scale 10 --seed 123 --query-type cpu-max-all-1         --file /tmp/bulk_data/cratedb_query_cpu-max-all-1
$GOPATH/bin/tsbs_generate_queries --format cratedb --use-case cpu-only --scale 10 --seed 123 --query-type high-cpu-1            --file /tmp/bulk_data/cratedb_query_high-cpu-1
$GOPATH/bin/tsbs_generate_queries --format cratedb --use-case cpu-only --scale 10 --seed 123 --query-type single-groupby-5-1-1  --file /tmp/bulk_data/cratedb_query_single-groupby-5-1-1
$GOPATH/bin/tsbs_generate_queries --format cratedb --use-case cpu-only --scale 10 --seed 123 --query-type groupby-orderby-limit --file /tmp/bulk_data/cratedb_query_groupby-orderby-limit

# insert benchmark
$GOPATH/bin/tsbs_load_cratedb --db-name=benchmark --hosts=localhost --workers=1 --file=/tmp/bulk_data/cratedb_data

# queries benchmark
$GOPATH/bin/tsbs_run_queries_cratedb --db-name=benchmark --hosts=localhost --workers=1 --max-queries=10 --file=/tmp/bulk_data/cratedb_query_lastpoint
$GOPATH/bin/tsbs_run_queries_cratedb --db-name=benchmark --hosts=localhost --workers=1 --max-queries=10 --file=/tmp/bulk_data/cratedb_query_cpu-max-all-1
$GOPATH/bin/tsbs_run_queries_cratedb --db-name=benchmark --hosts=localhost --workers=1 --max-queries=10 --file=/tmp/bulk_data/cratedb_query_high-cpu-1
$GOPATH/bin/tsbs_run_queries_cratedb --db-name=benchmark --hosts=localhost --workers=1 --max-queries=10 --file=/tmp/bulk_data/cratedb_query_single-groupby-5-1-1
$GOPATH/bin/tsbs_run_queries_cratedb --db-name=benchmark --hosts=localhost --workers=1 --max-queries=10 --file=/tmp/bulk_data/cratedb_query_groupby-orderby-limit
