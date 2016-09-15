#!/bin/bash
set -x 
binName=$(which bulk_query_gen)
binVersion=`md5sum $binName | awk '{ print $1 }'`
dataDir=${dataDir:-/disk/1/queries}

formats=${formats:-"influx-http cassandra iobeamdb"}
queryTypes=${queryTypes:-"single-host groupby 8-hosts lastpoint multiple-ors-by-host cpu-max-all-eight-hosts groupby high-cpu high-cpu-and-field multiple-ors cpu-max-all-single-host"}

scaleVar=${scaleVar:-"1000"}
queries=${queries:-"2500"}
seed=${seed:-"123"}
tsStart=${tsStart:-"2016-01-01T00:00:00Z"}
tsEnd=${tsEnd:-"2016-01-02T00:00:01Z"}
useCase=${useCase:-"devops"}

mkdir -p ${dataDir}
chmod a+rwx ${dataDir}

pushd ${dataDir}

for queryType in ${queryTypes}; do
    for format in ${formats}; do
        fname="queries_${format}_${queryType}_${binVersion}_${queries}_${scaleVar}_${seed}_${tsStart}_${tsEnd}_${useCase}.dat.gz"
        echo "$fname"
        if [ ! -f "$fname" ]; then
            $binName -format $format -queries $queries -query-type $queryType -scale-var $scaleVar -seed $seed -timestamp-end $tsEnd -timestamp-start $tsStart -use-case $useCase | gzip  > $fname
	    symlink=$format-${queryType}-queries.gz
            rm $symlink 2> /dev/null
            ln -s $fname $symlink
	    chmod a+rw $fname $symlink
        else
            echo "File exists"
        fi
    done
done 
