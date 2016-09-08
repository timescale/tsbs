#!/bin/bash

binName="./bulk_data_gen"
binVersion=`md5sum $binName | awk '{ print $1 }'`
dataDir="datafiles"


declare -a formats=("influx-bulk" "cassandra" "iobeamdb")
logSeconds="10s"
scaleVar="100"
seed="123"
tsStart="2016-01-01T00:00:00Z"
tsEnd="2016-01-01T00:10:00Z"
useCase="devops"

for format in "${formats[@]}"
do
    fname="${dataDir}/import_${format}_${binVersion}_${logSeconds}_${scaleVar}_${seed}_${tsStart}_${tsEnd}_${useCase}.dat"
    echo "$fname"
    if [ ! -f "$fname" ]; then
        $binName -format $format -logSeconds $logSeconds -scale-var $scaleVar -seed $seed -timestamp-end $tsEnd -timestamp-start $tsStart -use-case $useCase > $fname
    fi
   # or do whatever with individual element of the array
done


  # -format string
  #   	Format to emit. (choices: influx-bulk, es-bulk, cassandra, iobeamdb) (default "influx-bulk")
  # -logSeconds duration
  #   	duration between host data points (default 10s)
  # -scale-var int
  #   	Scaling variable specific to the use case. (default 1)
  # -seed int
  #   	PRNG seed (default, or 0, uses the current timestamp).
  # -timestamp-end string
  #   	Ending timestamp (RFC3339). (default "2016-01-02T06:00:00Z")
  # -timestamp-start string
  #   	Beginning timestamp (RFC3339). (default "2016-01-01T00:00:00Z")
  # -use-case string
  #   	Use case to model. (choices: devops, iot) (default "devops")
