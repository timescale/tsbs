#!/usr/bin/env bash
set -euf -o pipefail
database="benchmark_db"
measurement=${1}
field=${2}
start_date=${3}
end_date=${4}
interval=${5}

#query="q=SELECT count(${field}), min(${field}), max(${field}), mean(${field}), sum(${field}) FROM ${measurement}"
#echo $query
#curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=${database}" --data-urlencode "${query}"

query="q=SELECT count(${field}) FROM ${measurement} WHERE time >= '${start_date}' AND time < '${end_date}' GROUP BY time(${interval})"
echo $query
curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=${database}" --data-urlencode "${query}"
