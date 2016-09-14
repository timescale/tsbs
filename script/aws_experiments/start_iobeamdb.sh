#!/bin/bash

DATA_DIR=${DATA_DIR:-/disk}
POSTGRES_VERSION=${POSTGRES_VERSION:-master}
POSTGRES_DATA_DIR=${POSTGRES_DATA_DIR:-${DATA_DIR}/1/iobeamdb/data}
POSTGRES_WAL_DIR=${POSTGRES_WAL_DIR:-${DATA_DIR}/2/iobeamdb/wal}

docker stop postgres &>/dev/null

MEM=${MEM:-50000}
let "SHARED=$MEM/3"
let "CACHE=$MEM/2"
let "WORK=($MEM-$SHARED)/30"
let "MAINT=$MEM/10"
#  shared buffers = mem / 3 = 17G
#  effective_cache_size = mem / 2 = 25GB
#  work_mem =  (mem - shared_buffers) / (max_connections * 3) 
#           =      33G / 30 = 1G
#  maintenance_work_mem = mem / 10 = 5GB
#  


docker run -d --name postgres -p 5432:5432 -e POSTGRES_DB=test  -m ${MEM}m \
  -e "NO_BACKUPS=1" \
  -e "PGWAL=/data/wal/pgwal" \
  -e "POSTGRES_INITDB_ARGS=--xlogdir=/data/wal/pgwal" \
  -e "PGDATA=/var/lib/postgresql/data/pgdata" \
  -v ${POSTGRES_WAL_DIR}:/data/wal \
  -v ${POSTGRES_DATA_DIR}:/var/lib/postgresql/data \
  registry.iobeam.com/iobeam/postgres-9.5-wale:${POSTGRES_VERSION} postgres \
  -cmax_locks_per_transaction=1000 \
  -cshared_preload_libraries=pg_stat_statements \
  -cvacuum_cost_delay=20 -cautovacuum_max_workers=1 \
  -clog_autovacuum_min_duration=1000 \
  -ciobeam.hostname=local -clog_line_prefix="%m [%p]: [%l-1] %u@%d" \
  -cshared_buffers=${SHARED}MB \
  -ceffective_cache_size=${CACHE}MB \
  -cwork_mem=${WORK}MB \
  -cmaintenance_work_mem=${MAINT}MB 
