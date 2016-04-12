#!/usr/bin/env sh
set -euf -o pipefail
database=${1}
measurement=${2}
field=${3}

query="q=SELECT count(${field}), min(${field}), max(${field}), mean(${field}), sum(${field}) FROM ${measurement}"
echo $query
curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=${database}" --data-urlencode "${query}"

query="q=SELECT count(${field}) FROM ${measurement} WHERE time >= '2016-01-01' GROUP BY time(1w)"
echo $query
curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=${database}" --data-urlencode "${query}"
