#!/bin/bash
set -x
binName=$(which tsbs_generate_queries)
binVersion=`md5sum $binName | awk '{ print $1 }'`
dataDir=${dataDir:-/tmp/queries}
useJson=${useJson:-false}
useTags=${useTags:-true}

formats=${formats:-"timescaledb"}
queryTypes=${queryTypes:-"1-host-1-hr 1-host-12-hr 8-host-1-hr groupby groupby-5 groupby-all lastpoint cpu-max-all-single-host cpu-max-all-eight-hosts high-cpu-all-hosts high-cpu-1-host groupby-orderby-limit 5-metrics-1-host-1-hr 5-metrics-1-host-12-hr 5-metrics-8-host-1-hr"}

scaleVar=${scaleVar:-"1000"}
queries=${queries:-"2500"}
seed=${seed:-"123"}
tsStart=${tsStart:-"2016-01-01T00:00:00Z"}
tsEnd=${tsEnd:-"2016-01-04T00:00:01Z"}
useCase=${useCase:-"cpu-only"}

mkdir -p ${dataDir}
chmod a+rwx ${dataDir}

pushd ${dataDir}

for queryType in ${queryTypes}; do
    for format in ${formats}; do
        fname="queries_${format}_${queryType}_${binVersion}_${queries}_${scaleVar}_${seed}_${tsStart}_${tsEnd}_${useCase}.dat.gz"
        echo "$fname"
        if [ ! -f "$fname" ]; then
            $binName -format $format -queries $queries -query-type $queryType -scale-var $scaleVar -seed $seed -timestamp-end $tsEnd -timestamp-start $tsStart -use-case $useCase -timescale-use-json=$useJson -timescale-use-tags=$useTags | gzip  > $fname
	    symlink=$format-${queryType}-queries.gz
            rm $symlink 2> /dev/null
            ln -s $fname $symlink
	    chmod a+rw $fname $symlink
        else
            echo "File exists"
        fi
    done
done
