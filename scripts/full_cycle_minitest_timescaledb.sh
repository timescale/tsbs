#!/bin/bash

$GOPATH/bin/tsbs_generate_data --format timescaledb --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/timescaledb_data

$GOPATH/bin/tsbs_generate_queries --format timescaledb --use-case cpu-only --scale 10 --seed 123 --query-type lastpoint     --file /tmp/bulk_data/timescaledb_query_lastpoint
$GOPATH/bin/tsbs_generate_queries --format timescaledb --use-case cpu-only --scale 10 --seed 123 --query-type cpu-max-all-1 --file /tmp/bulk_data/timescaledb_query_cpu-max-all-1
$GOPATH/bin/tsbs_generate_queries --format timescaledb --use-case cpu-only --scale 10 --seed 123 --query-type high-cpu-1    --file /tmp/bulk_data/timescaledb_query_high-cpu-1

$GOPATH/bin/tsbs_load_timescaledb --postgres="sslmode=disable port=5433" --db-name=benchmark --host=127.0.0.1 --user=postgres --workers=1 --file=/tmp/bulk_data/timescaledb_data

$GOPATH/bin/tsbs_run_queries_timescaledb --postgres="sslmode=disable port=5433" --db-name=benchmark --hosts=127.0.0.1 --user=postgres --workers=1 --max-queries=100 --file=/tmp/bulk_data/timescaledb_query_lastpoint
$GOPATH/bin/tsbs_run_queries_timescaledb --postgres="sslmode=disable port=5433" --db-name=benchmark --hosts=127.0.0.1 --user=postgres --workers=1 --max-queries=100 --file=/tmp/bulk_data/timescaledb_query_cpu-max-all-1
$GOPATH/bin/tsbs_run_queries_timescaledb --postgres="sslmode=disable port=5433" --db-name=benchmark --hosts=127.0.0.1 --user=postgres --workers=1 --max-queries=100 --file=/tmp/bulk_data/timescaledb_query_high-cpu-1
