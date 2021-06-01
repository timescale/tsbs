#!/usr/bin/env sh
docker run -d --name timescaledb-experiments -e "POSTGRES_HOST_AUTH_METHOD=trust" --publish=5432:5432 timescale/timescaledb:latest-pg12

