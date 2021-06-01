#!/usr/bin/env sh
echo "--workers=10 --batch-size=125"
cat /tmp/bulk_data/clickhouse-data.gz | gunzip | ../../bin/tsbs_load_clickhouse --user="default" --password="" --do-create-db=true --host="localhost" --db-name="benchmark" --workers=10 --batch-size=125

:<<-EOF
--workers=10 --batch-size=125
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
1622539336,851867.85,8.518750E+06,851867.85,85186.79,8.518750E+05,85186.79
1622539346,885124.28,1.737000E+07,868496.00,88512.43,1.737000E+06,86849.60
1622539356,859369.70,2.596375E+07,865453.90,85936.97,2.596375E+06,86545.39
1622539366,921501.91,3.517875E+07,879465.82,92150.19,3.517875E+06,87946.58
1622539376,870251.11,4.388125E+07,877622.89,87025.11,4.388125E+06,87762.29

Summary:
loaded 50000000 metrics in 57.647sec with 10 workers (mean rate 867345.42 metrics/sec)
loaded 5000000 rows in 57.647sec with 10 workers (mean rate 86734.54 rows/sec)

select * from hypertable_detailed_size('cpu') ORDER BY node_name;
 table_bytes | index_bytes | toast_bytes | total_bytes | node_name
-------------+-------------+-------------+-------------+-----------
   705765376 |  2422562816 |     1720320 |  3130056704 |
EOF
