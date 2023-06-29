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
