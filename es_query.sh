#!/usr/bin/env bash
set -euf -o pipefail
measurement=${1}
field=${2}
start_date=${3}
end_date=${4}
interval=${5}
#query="
#{
#  \"size\": 0,
#  \"aggs\": {
#        \"my_stats\" : { \"stats\" : { \"field\" : \"${field}\" } }
#  }
#}
#"
#
#echo $query
#echo $query | curl -XPOST "localhost:9200/${measurement}/transactions/_search?pretty" -d @-

query="
{
  \"size\" : 0,
  \"aggs\": {
    \"result\": {
      \"filter\": {
        \"range\": {
          \"timestamp\": {
            \"gte\": \"${start_date}\",
            \"lt\": \"${end_date}\"
          }
        }
      },
      \"aggs\": {
        \"result2\": {
          \"date_histogram\": {
            \"field\": \"timestamp\",
            \"interval\": \"${interval}\",
            \"format\": \"yyyy-MM-dd-HH\"
          }
        }
      }
    }
  }
}
"
echo $query
#echo $query | curl -G "localhost:9200/${measurement}/_search?pretty" -d @-
echo $query | curl "localhost:9200/${measurement}/_search?pretty" -d @-
