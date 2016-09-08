#remove all docker instances
docker rm -vf $(docker ps -a -q)

sudo rm -rf /disk/2/iobeam/wal/*
sudo rm -rf /disk/1/iobeam/data/*

docker run -d --name postgres -p 5432:5432 -e POSTGRES_DB=test  -m 52g \
  -e "NO_BACKUPS=1" \
  -e "PGWAL=/data/wal/pgwal" \
  -e "POSTGRES_INITDB_ARGS=--xlogdir=/data/wal/pgwal" \
  -e "PGDATA=/var/lib/postgresql/data/pgdata" \
  -v /disk/2/iobeam/wal:/data/wal \
  -v /disk/1/iobeam/data:/var/lib/postgresql/data \
  registry.iobeam.com/iobeam/postgres-9.5-wale:master  postgres \
  -cmax_locks_per_transaction=1000 \
  -cshared_preload_libraries=pg_stat_statements \
  -cvacuum_cost_delay=20 -cautovacuum_max_workers=1 \
  -clog_autovacuum_min_duration=1000 \
  -ciobeam.hostname=local -clog_line_prefix="%m [%p]: [%l-1] %u@%d" \
  -cshared_buffers=17GB \
  -ceffective_cache_size=25GB \
  -cwork_mem=1GB \
  -cmaintenance_work_mem=5GB 
