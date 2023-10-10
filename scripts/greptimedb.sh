BENCH_DIR=bench-greptime

# setup
make
mkdir $BENCH_DIR
cat "[storage] data_home = \"$(PWD $BENCH_DIR)/data\"" >> $BENCH_DIR/config.toml
greptime standalone start -c $BENCH_DIR/config.toml

# generate data
./bin/tsbs_generate_data \
    --use-case=cpu-only \
    --seed=123 \
    --scale=4000 \
    --timestamp-start="2023-06-11T00:00:00Z" \
    --timestamp-end="2023-06-14T00:00:00Z" \
    --log-interval=10s \
    --format=influx > ./$BENCH_DIR/bench-data.lp

# load data
./bin/tsbs_load_greptime \
    --urls=http://localhost:4000 \
    --file=./$BENCH_DIR/influx-data.lp \
    --batch-size=3000 \
    --gzip=false \
    --workers=6

# generate query
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

QUERY_TYPES=${QUERY_TYPES:-$QUERY_TYPES_ALL}

for QUERY_TYPE in ${QUERY_TYPES}; do
    ./tsbs_generate_queries \
        --use-case="devops" --seed=123 --scale=4000 \
        --timestamp-start="2023-06-11T00:00:00Z" \
        --timestamp-end="2023-06-14T00:00:01Z" \
        --queries=10 \
        --query-type ${QUERY_TYPE} \
        --format="greptime" \
        > ./queries/greptime-queries-${QUERY_TYPE}.dat
done

# run query
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

QUERY_TYPES=${QUERY_TYPES:-$QUERY_TYPES_ALL}

for QUERY_TYPE in ${QUERY_TYPES}; do
    /home/greptime/tsbs-benchmark/tsbs/bin/tsbs_run_queries_influx --file=./queries/greptime-queries-${QUERY_TYPE}.dat \
        --db-name=benchmark \
        --urls="http://localhost:4000"
done
