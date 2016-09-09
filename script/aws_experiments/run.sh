#!/bin/bash
set -e
set -x

# set GOPATH
BASE_DIR=${BASE_DIR:-/disk/1}
FORMAT=${FORMAT:-iobeam}


BENCHMARK_DIR=${BENCHMARK_DIR:-"${GOPATH}/src/bitbucket.org/440-labs/influxdb-comparisons/"}
SCALE_VAR=${SCALE_VAR:-32}
START_REMOTE=${START_REMOTE:-true}
DATA_DIR=${DATA_DIR:-${BASE_DIR}/${FORMAT}}
POSTGRES_DB="benchmark"
POSTGRES_CONNECT="host=database user=postgres sslmode=disable"
POSTGRES_SCRIPTS_DIR=${POSTGRES_SCRIPTS_DIR:-"$GOPATH/src/bitbucket.org/440-labs/postgres-kafka-consumer/sql/scripts/"}

INFLUX_URL=${INFLUX_URL:-"http://database:8086"}

FORMAT_DATA_GEN=$FORMAT
case $FORMAT in 
  influx)
    FORMAT_DATA_GEN="influx-bulk"
    ;;
esac

FORMAT_QUERY_GEN=$FORMAT
case $FORMAT in 
  influx)
    FORMAT_QUERY_GEN="influx-http"
    ;;
esac


if [ "$START_REMOTE" = true ]; then
ssh database <<'ENDSSH'
  #remove all docker instances

  ~/start_postgres.sh

docker ps
ENDSSH
fi

mkdir -p $DATA_DIR

cd $BENCHMARK_DIR
cd bulk_data_gen
go build 
./bulk_data_gen -format $FORMAT_DATA_GEN --seed=123 --use-case=devops --scale-var=$SCALE_VAR  |gzip > $DATA_DIR/data.gz

case "$FORMAT" in 
  iobeam)
    cd $BENCHMARK_DIR
    cd bulk_load_iobeam/
    go build 
    cat $DATA_DIR/data.gz | gunzip | ./bulk_load_iobeam \
      --scriptsDir=$POSTGRES_SCRIPTS_DIR \
      --postgres="$POSTGRES_CONNECT" \
      --batch-size=5000 --workers=2 --tag-index="VALUE-TIME" --field-index=""
    ;;
  influx)
    cd $BENCHMARK_DIR
    cd bulk_load_influx/
    go build 
    cat $DATA_DIR/data.gz | gunzip | ./bulk_load_influx \
      --url $INFLUX_URL --workers=2 \
    ;;
esac

cd $BENCHMARK_DIR
cd bulk_query_gen/
go build
./bulk_query_gen --debug=0 --seed=321 --format=$FORMAT_QUERY_GEN \
  --query-type=lastpoint --scale-var=$SCALE_VAR | gzip > $DATA_DIR/query-lastpoint.gz
./bulk_query_gen --debug=0 --seed=321 --format=$FORMAT_QUERY_GEN \
  --query-type=8-hosts --scale-var=$SCALE_VAR | gzip > $DATA_DIR/query-8-hosts.gz
./bulk_query_gen --debug=0 --seed=321 --format=$FORMAT_QUERY_GEN \
  --query-type=single-host --scale-var=$SCALE_VAR | gzip > $DATA_DIR/query-single-host.gz
./bulk_query_gen --debug=0 --seed=321 --format=$FORMAT_QUERY_GEN \
  --query-type=groupby --scale-var=$SCALE_VAR | gzip > $DATA_DIR/query-groupby.gz

cd $BENCHMARK_DIR

case "$FORMAT" in 
  iobeam)
    cd query_benchmarker_iobeam/
    OPT="$POSTGRES_CONNECT dbname=$POSTGRES_DB"
    CMD="./query_benchmarker_iobeam \
  --workers=2 \
  --postgres='$OPT'"
    ;;
  influx)
    cd query_benchmarker_influxdb/
    CMD="./query_benchmarker_influxdb \
  --workers=2 \
  --url=$INFLUX_URL"
    ;;
esac

go build

cat $DATA_DIR/query-lastpoint.gz| gunzip | eval $CMD 
cat $DATA_DIR/query-8-hosts.gz| gunzip | eval $CMD 
cat $DATA_DIR/query-single-host.gz| gunzip | eval $CMD 
cat $DATA_DIR/query-groupby.gz| gunzip | eval $CMD 

echo "done"



