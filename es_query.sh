#!/usr/bin/env sh
set -euf -o pipefail
measurement=${1}
field=${2}
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
   \"filter\": {
     \"range\": {
       \"timestamp\": {
         \"gte\": \"2016-01-01\",
         \"lt\": \"2016-01-15\"
       }
     }
   },
   \"aggs\": {
      \"result\": {
         \"date_histogram\": {
            \"field\": \"timestamp\",
            \"interval\": \"1h\",
            \"format\": \"yyyy-MM-dd-HH\"
         }
      }
   }
}
"
echo $query
#echo $query | curl -G "localhost:9200/${measurement}/_search?pretty" -d @-
echo $query | curl "localhost:9200/${measurement}/_search?pretty" -d @-
