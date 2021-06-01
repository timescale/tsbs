#!/usr/bin/env sh
echo "--workers=1 --partitions=10 --chunk-time=5m --batch-size=125"
cat /tmp/bulk_data/timescaledb-data.gz | gunzip | ../../bin/tsbs_load_timescaledb --do-create-db=true --host="localhost" --db-name="benchmark" --workers=1 --batch-size=125 --field-index="VALUE-TIME" --time-partition-index=true --partitions=10 --chunk-time="5m" --field-index-count="-1"

<<-EOF
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
1622546699,131999.16,1.320000E+06,131999.16,13199.92,1.320000E+05,13199.92
1622546709,120375.22,2.523750E+06,126187.22,12037.52,2.523750E+05,12618.72
1622546719,139248.43,3.916250E+06,130540.98,13924.84,3.916250E+05,13054.10
1622546729,131250.86,5.228750E+06,130718.45,13125.09,5.228750E+05,13071.85
1622546739,133749.88,6.566250E+06,131324.74,13374.99,6.566250E+05,13132.47
1622546749,136624.53,7.932500E+06,132208.04,13662.45,7.932500E+05,13220.80
1622546759,133370.22,9.266250E+06,132374.07,13337.02,9.266250E+05,13237.41

You are now connected to database "benchmark" as user "postgres".
benchmark=# select * from hypertable_detailed_size('cpu') ORDER BY node_name;
 table_bytes | index_bytes | toast_bytes | total_bytes | node_name
-------------+-------------+-------------+-------------+-----------
   134873088 |   514506752 |      737280 |   650125312 |
(1 row)

10% more space doubling the partitions
EOF
