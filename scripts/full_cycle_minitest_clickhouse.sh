#!/bin/bash

MAX_RPS=${MAX_RPS:-"0"}
MAX_QUERIES=${MAX_QUERIES:-"1000"}
PASSWORD=${PASSWORD:-""}

$GOPATH/bin/tsbs_generate_data --format clickhouse --use-case cpu-only --scale 10 --seed 123 --file /tmp/bulk_data/clickhouse_data

$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format clickhouse --use-case cpu-only --scale 10 --seed 123 --query-type lastpoint     --file /tmp/bulk_data/clickhouse_query_lastpoint
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format clickhouse --use-case cpu-only --scale 10 --seed 123 --query-type cpu-max-all-1 --file /tmp/bulk_data/clickhouse_query_cpu-max-all-1
$GOPATH/bin/tsbs_generate_queries --queries=${MAX_QUERIES} --format clickhouse --use-case cpu-only --scale 10 --seed 123 --query-type high-cpu-1    --file /tmp/bulk_data/clickhouse_query_high-cpu-1

$GOPATH/bin/tsbs_load_clickhouse --db-name=benchmark --host=127.0.0.1 --workers=1 --file=/tmp/bulk_data/clickhouse_data
#last point query is broke
#$GOPATH/bin/tsbs_run_queries_clickhouse --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_clickhouse_query_lastpoint.hdr" --db-name=benchmark --hosts=127.0.0.1 --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/clickhouse_query_lastpoint
$GOPATH/bin/tsbs_run_queries_clickhouse --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_clickhouse_query_cpu-max-all-1.hdr" --db-name=benchmark --hosts=127.0.0.1 --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/clickhouse_query_cpu-max-all-1
$GOPATH/bin/tsbs_run_queries_clickhouse --max-rps=${MAX_RPS} --hdr-latencies="${MAX_RPS}rps_clickhouse_query_high-cpu-1.hdr" --db-name=benchmark --hosts=127.0.0.1 --workers=1 --max-queries=${MAX_QUERIES} --file=/tmp/bulk_data/clickhouse_query_high-cpu-1


