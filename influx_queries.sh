#!/usr/bin/env sh
set -euf -o pipefail
database=${1}
measurement=${2}
field=${3}

curl -G 'http://localhost:8086/query?pretty=true' --data-urlencode "db=${database}" --data-urlencode "q=SELECT count(${field}), min(${field}), max(${field}), mean(${field}), sum(${field}) FROM ${measurement}"
