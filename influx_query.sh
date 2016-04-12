#!/usr/bin/env sh
set -euf -o pipefail
database="benchmark_db"
measurement=${1}
field=${2}

#query="q=SELECT count(${field}), min(${field}), max(${field}), mean(${field}), sum(${field}) FROM ${measurement}"
#echo $query
#curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=${database}" --data-urlencode "${query}"

query="q=SELECT count(${field}) FROM ${measurement} WHERE time >= '2016-01-01' AND time < '2016-01-15' GROUP BY time(1h)"
#echo $query
curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=${database}" --data-urlencode "${query}"
