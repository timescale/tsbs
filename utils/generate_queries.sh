#!/bin/bash
set -x 
binName="./bulk_query_gen"
binVersion=`md5sum $binName | awk '{ print $1 }'`
dataDir="datafiles"

declare -a formats=("influx-http" "cassandra" "iobeam")
declare -a queryTypes=("single-host" "groupby" "8-hosts")

scaleVar="100"
queries="1000"
seed="123"
tsStart="2016-01-01T00:00:00Z"
tsEnd="2016-01-02T06:00:00Z"
useCase="devops"

for queryType in "${queryTypes[@]}"; do
    for format in "${formats[@]}"; do
        fname="${dataDir}/queries_${format}_${queryType}_${binVersion}_${queries}_${scaleVar}_${seed}_${tsStart}_${tsEnd}_${useCase}.dat"
        echo "$fname"
        if [ ! -f "$fname" ]; then
            $binName -format $format -queries $queries -query-type $queryType -scale-var $scaleVar -seed $seed -timestamp-end $tsEnd -timestamp-start $tsStart -use-case $useCase > $fname
        else
            echo "File exists"
        fi
    done
done 
