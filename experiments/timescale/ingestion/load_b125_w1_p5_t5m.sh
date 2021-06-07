#!/usr/bin/env sh
echo "--workers=1 --partitions=5 --chunk-time=5m --batch-size=125"
cat /tmp/bulk_data/timescaledb-data.gz | gunzip | ../../../bin/tsbs_load_timescaledb --do-create-db=true --host="localhost" --db-name="benchmark" --workers=1 --batch-size=125 --field-index="VALUE-TIME" --time-partition-index=true --partitions=5 --chunk-time="5m" --field-index-count="-1"

<<-EOF
--workers=1 --partitions=5 --time-partition-index=true --batch-size=125
1622543617,138748.14,1.387500E+06,138748.14,13874.81,1.387500E+05,13874.81
1622543627,145747.72,2.845000E+06,142247.94,14574.77,2.845000E+05,14224.79
1622543637,143744.35,4.282500E+06,142746.75,14374.43,4.282500E+05,14274.67
1622543647,141869.76,5.701250E+06,142527.50,14186.98,5.701250E+05,14252.75
1622543657,141886.96,7.120000E+06,142399.40,14188.70,7.120000E+05,14239.94
1622543667,142500.30,8.545000E+06,142416.22,14250.03,8.545000E+05,14241.62
1622543677,135500.52,9.900000E+06,141428.27,13550.05,9.900000E+05,14142.83

Summary:
loaded 10000000 metrics in 70.709sec with 1 workers (mean rate 141424.84 metrics/sec)
loaded 1000000 rows in 70.709sec with 1 workers (mean rate 14142.48 rows/sec)

benchmark=# select * from hypertable_detailed_size('cpu') ORDER BY node_name;
 table_bytes | index_bytes | toast_bytes | total_bytes | node_name
-------------+-------------+-------------+-------------+-----------
   135602176 |   469278720 |      368640 |   605257728 |
EOF
