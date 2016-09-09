#remove all docker instances
docker rm -vf $(docker ps -a -q)

BASE_DATA_DIR=${BASE_DATA_DIR:-/disk}
DATA_DIR=${DATA_DIR:-${BASE_DATA_DIR}/1/influxdb/data}
WAL_DIR=${WAL_DIR:-${BASE_DATA_DIR}/2/influxdb/wal}

rm -rf ${DATA_DIR} ${WAL_DIR}

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
  -v ${WAL_DIR}:/data/wal \
  -v ${DATA_DIR}:/var/lib/postgresql/data \
  registry.iobeam.com/iobeam/postgres-9.5-wale:master  postgres \
  -cmax_locks_per_transaction=1000 \
  -cshared_preload_libraries=pg_stat_statements \
  -cvacuum_cost_delay=20 -cautovacuum_max_workers=1 \
  -clog_autovacuum_min_duration=1000 \
  -ciobeam.hostname=local -clog_line_prefix="%m [%p]: [%l-1] %u@%d" \
  -cshared_buffers=${SHARED}MB \
  -ceffective_cache_size=${CACHE}MB \
  -cwork_mem=${WORK}MB \
  -cmaintenance_work_mem=${MAINT}MB 
