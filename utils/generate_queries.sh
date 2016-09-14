#!/bin/bash
set -x 
binName=$(which bulk_query_gen)
binVersion=`md5sum $binName | awk '{ print $1 }'`
dataDir=${dataDir:-/disk/1/queries}

declare -a formats=("influx-http" "cassandra" "iobeamdb")
declare -a queryTypes=("single-host" "groupby" "8-hosts")

scaleVar=${scaleVar:-"1000"}
queries=${queries:-"2500"}
seed=${seed:-"123"}
tsStart=${tsStart:-"2016-01-01T00:00:00Z"}
tsEnd=${tsEnd:-"2016-01-02T00:00:01Z"}
useCase=${useCase:-"devops"}

mkdir -p ${dataDir}

pushd ${dataDir}

for queryType in "${queryTypes[@]}"; do
    for format in "${formats[@]}"; do
        fname="queries_${format}_${queryType}_${binVersion}_${queries}_${scaleVar}_${seed}_${tsStart}_${tsEnd}_${useCase}.dat.gz"
        echo "$fname"
        if [ ! -f "$fname" ]; then
            $binName -format $format -queries $queries -query-type $queryType -scale-var $scaleVar -seed $seed -timestamp-end $tsEnd -timestamp-start $tsStart -use-case $useCase | gzip  > $fname
            rm $format-${queryType}-queries.gz
            ln -s $fname $format-${queryType}-queries.gz
        else
            echo "File exists"
        fi
    done
done 
