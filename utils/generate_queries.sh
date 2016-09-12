#!/bin/bash
set -x 
binName=$(which bulk_query_gen)
binVersion=`md5sum $binName | awk '{ print $1 }'`
dataDir=${dataDir:-datafiles}

declare -a formats=("influx-http" "cassandra" "iobeam")
declare -a queryTypes=("single-host" "groupby" "8-hosts")

scaleVar=${scaleVar:-"100"}
queries=${queries:-"1000"}
seed=${seed:-"123"}
tsStart=${tsStart:-"2016-01-01T00:00:00Z"}
tsEnd=${tsEnd:-"2016-01-02T06:00:00Z"}
useCase=${useCase:-"devops"}

mkdir -p ${dataDir}

pushd ${dataDir}

for queryType in "${queryTypes[@]}"; do
    for format in "${formats[@]}"; do
        fname="queries_${format}_${queryType}_${binVersion}_${queries}_${scaleVar}_${seed}_${tsStart}_${tsEnd}_${useCase}.dat.gz"
        echo "$fname"
        if [ ! -f "$fname" ]; then
            $binName -format $format -queries $queries -query-type $queryType -scale-var $scaleVar -seed $seed -timestamp-end $tsEnd -timestamp-start $tsStart -use-case $useCase | gzip  > $fname
            ln -s $fname $format-${queryType}-queries.gz
        else
            echo "File exists"
        fi
    done
done 
