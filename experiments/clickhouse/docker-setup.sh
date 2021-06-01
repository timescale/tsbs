#!/usr/bin/env sh
docker run -d --name clickhouse-experiments --ulimit nofile=262144:262144 --publish=9000:9000 yandex/clickhouse-server
