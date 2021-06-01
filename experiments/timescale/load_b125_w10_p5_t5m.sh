#!/usr/bin/env sh
echo "--workers=10 --partitions=5 --chunk-time=5m --batch-size=125"
cat /tmp/bulk_data/timescaledb-data.gz | gunzip | ../../bin/tsbs_load_timescaledb --do-create-db=true --host="localhost" --db-name="benchmark" --workers=10 --batch-size=125 --field-index="VALUE-TIME" --time-partition-index=true --partitions=5 --chunk-time="5m" --field-index-count="-1"

<<-EOF
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
1622549719,599964.16,6.000000E+06,599964.16,59996.42,6.000000E+05,59996.42
1622549729,607146.60,1.207125E+07,603555.21,60714.66,1.207125E+06,60355.52
1622549739,598253.88,1.805375E+07,601788.12,59825.39,1.805375E+06,60178.81
1622549749,626127.25,2.431500E+07,607872.86,62612.72,2.431500E+06,60787.29
1622549759,584619.89,3.016125E+07,603222.24,58461.99,3.016125E+06,60322.22
1622549769,578996.34,3.595125E+07,599184.59,57899.63,3.595125E+06,59918.46
1622549779,573382.40,4.168500E+07,595498.62,57338.24,4.168500E+06,59549.86
1622549789,585123.36,4.753625E+07,594201.71,58512.34,4.753625E+06,59420.17

Summary:
loaded 50000000 metrics in 86.181sec with 10 workers (mean rate 580177.20 metrics/sec)
loaded 5000000 rows in 86.181sec with 10 workers (mean rate 58017.72 rows/sec)

select * from hypertable_detailed_size('cpu') ORDER BY node_name;
 table_bytes | index_bytes | toast_bytes | total_bytes | node_name
-------------+-------------+-------------+-------------+-----------
   705765376 |  2422562816 |     1720320 |  3130056704 |
EOF
