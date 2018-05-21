#!/bin/bash
set -x
binName=$(which tsbs_generate_queries)
binVersion=`md5sum $binName | awk '{ print $1 }'`
dataDir=${dataDir:-/tmp/queries}
useJson=${useJson:-false}
useTags=${useTags:-true}

formats=${formats:-"timescaledb"}
queryTypes=${queryTypes:-"single-groupby(1,1,1) single-groupby(1,1,12) single-groupby(1,8,1) single-groupby(5,1,1) single-groupby(5,1,12) single-groupby(5,8,1) double-groupby(1) double-groupby(5) double-groupby(all) cpu-max-all(1) cpu-max-all(8) high-cpu(all) high-cpu(1) lastpoint groupby-orderby-limit"}

scaleVar=${scaleVar:-"4000"}
queries=${queries:-"1000"}
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
