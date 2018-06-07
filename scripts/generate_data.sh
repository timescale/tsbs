#!/bin/bash

binName=$(which tsbs_generate_data)
dataDir=${dataDir:-/tmp}

formats=${formats:-"timescaledb"}
scaleVar=${scaleVar:-"4000"}
seed=${seed:-"123"}
tsStart=${tsStart:-"2016-01-01T00:00:00Z"}
tsEnd=${tsEnd:-"2016-01-04T00:00:00Z"}
useCase=${useCase:-"cpu-only"}
logInterval=${logInterval:-"10s"}

mkdir -p ${dataDir}

pushd ${dataDir}

for format in ${formats}
do
    fname="data_${format}_${useCase}_${scaleVar}_${tsStart}_${tsEnd}_${logInterval}_${seed}.dat.gz"
    echo "Generating $fname:"
    if [ ! -f "$fname" ]; then
        $binName -format $format -scale-var $scaleVar -seed $seed -timestamp-end $tsEnd -timestamp-start $tsStart -log-interval $logInterval -use-case $useCase | gzip > $fname
        ln -s $fname ${format}-data.gz
    fi
done
