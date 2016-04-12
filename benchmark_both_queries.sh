#!/usr/bin/env sh
echo 'influx:'
./benchmark_influx_queries.sh $@
echo ''
echo 'elasticsearch:'
./benchmark_es_queries.sh $@
