#!/bin/bash

PG_VER=${PG_VER:-10}
MEM=${MEM:-`free -m | grep "Mem" | awk '{print $7}'`}

let "SHARED=$MEM/4"
let "CACHE=2*$MEM/3"
let "WORK=($MEM-$SHARED)/30"
let "MAINT=$MEM/16"

sudo -u postgres /usr/lib/postgresql/${PG_VER}/bin/pg_ctl -c -U postgres -D /etc/postgresql/${PG_VER}/main -l /tmp/postgres.log -o "-cshared_preload_libraries=timescaledb \
 -clog_line_prefix=\"%m [%p]: [%x] %u@%d\" \
 -clogging_collector=off \
 -csynchronous_commit=off \
 -cmax_wal_size=10GB \
 -cshared_buffers=${SHARED}MB \
 -ceffective_cache_size=${CACHE}MB \
 -cwork_mem=${WORK}MB \
 -cmaintenance_work_mem=${MAINT}MB \
 -cmax_files_per_process=100 \
 -cautovacuum=on" start
