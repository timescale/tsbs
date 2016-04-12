#!/usr/bin/env sh
set -euf -o pipefail
measurement=${1}
field=${2}
query="
{
  \"size\": 0,
  \"aggs\": {
        \"my_stats\" : { \"stats\" : { \"field\" : \"${field}\" } }
  }
}
"

echo $query
echo $query | curl -XPOST "localhost:9200/${measurement}/_search?pretty" -d @-
