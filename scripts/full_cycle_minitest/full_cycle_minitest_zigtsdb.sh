#!/usr/bin/env bash
# showcases the tsbs 3 phases for zigtsdb against a local server (default http://localhost:8080)
# - 1) data generation
# - 2) data loading/insertion
# - 3) query execution (every devops query type)
set -euo pipefail

BIN=${BIN:-$GOPATH/bin}
URLS=${URLS:-http://localhost:8080}
SCALE=${SCALE:-10}
# 60s keeps the default 3-day range under the free tier's 1M points/day
LOG_INTERVAL=${LOG_INTERVAL:-60s}
OUT=${OUT:-/tmp/bulk_data}
# lastpoint last: it needs zigtsdb >= 1.2.0 (last() aggregate)
QUERY_TYPES=${QUERY_TYPES:-"cpu-max-all-1 cpu-max-all-8 double-groupby-1 double-groupby-5 double-groupby-all groupby-orderby-limit high-cpu-1 high-cpu-all single-groupby-1-1-1 single-groupby-1-1-12 single-groupby-1-8-1 single-groupby-5-1-1 single-groupby-5-1-12 single-groupby-5-8-1 lastpoint"}

mkdir -p "$OUT"

# generate data (cpu-only: 10 hosts x 10 fields = 100 series)
"$BIN"/tsbs_generate_data --format zigtsdb --use-case cpu-only --scale "$SCALE" --seed 123 --log-interval "$LOG_INTERVAL" --file "$OUT"/zigtsdb_data

# generate queries
for qt in $QUERY_TYPES; do
    "$BIN"/tsbs_generate_queries --format zigtsdb --use-case cpu-only --scale "$SCALE" --seed 123 \
        --query-type "$qt" --file "$OUT"/zigtsdb_query_"$qt"
done

# insert benchmark
"$BIN"/tsbs_load_zigtsdb --file="$OUT"/zigtsdb_data --urls="$URLS" --workers=2 --batch-size=1000

# queries benchmark
for qt in $QUERY_TYPES; do
    echo "== $qt =="
    "$BIN"/tsbs_run_queries_zigtsdb --max-queries=10 --urls="$URLS" --file="$OUT"/zigtsdb_query_"$qt"
done
