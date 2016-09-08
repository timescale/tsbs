#!/bin/bash
set -e
set -x

BENCHMARK_DIR=${BENCHMARK_DIR:-"/home/arye/go/src/bitbucket.org/440-labs/influxdb-comparisons/"}
SCALE_VAR=${SCALE_VAR:-32}
POSTGRES_DB="benchmark"
POSTGRES_CONNECT="host=database user=postgres sslmode=disable"

export GOPATH=~/go

ssh database <<'ENDSSH'
#remove all docker instances

~/start_postgres.sh

docker ps

ENDSSH


cd $BENCHMARK_DIR
cd bulk_data_gen
go build && ./bulk_data_gen -format iobeamdb --seed=123 --use-case=devops --scale-var=$SCALE_VAR  |gzip > /disk/1/iobeam/data.gz

cd $BENCHMARK_DIR
cd bulk_load_iobeam/
go build && cat /disk/1/iobeam/data.gz | gunzip | ./bulk_load_iobeam \
  --scriptsDir="/home/arye/go/src/bitbucket.org/440-labs/postgres-kafka-consumer/sql/scripts/" \
  --postgres="$POSTGRES_CONNECT" \
  --batch-size=5000 --workers=2 --tag-index="VALUE-TIME" --field-index=""

cd $BENCHMARK_DIR
cd bulk_query_gen/
go build
./bulk_query_gen --debug=0 --seed=321 --format=iobeam \
  --query-type=8-hosts --scale-var=$SCALE_VAR | gzip > /disk/1/iobeam/query-8-hosts.gz
./bulk_query_gen --debug=0 --seed=321 --format=iobeam \
  --query-type=single-host --scale-var=$SCALE_VAR | gzip > /disk/1/iobeam/query-single-host.gz
./bulk_query_gen --debug=0 --seed=321 --format=iobeam \
  --query-type=groupby --scale-var=$SCALE_VAR | gzip > /disk/1/iobeam/query-groupby.gz

cd $BENCHMARK_DIR
cd query_benchmarker_iobeam/
go build
cat /disk/1/iobeam/query-8-hosts.gz| gunzip |  ./query_benchmarker_iobeam \
  --workers=2 \
  --postgres="$POSTGRES_CONNECT dbname=$POSTGRES_DB" 
cat /disk/1/iobeam/query-single-host.gz| gunzip |  ./query_benchmarker_iobeam \
  --workers=2 \
  --postgres="$POSTGRES_CONNECT dbname=$POSTGRES_DB" 
cat /disk/1/iobeam/query-groupby.gz| gunzip |  ./query_benchmarker_iobeam \
  --workers=2 \
  --postgres="$POSTGRES_CONNECT dbname=$POSTGRES_DB"

echo "done"



