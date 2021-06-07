#!/usr/bin/env sh
echo "--workers=1 --partitions=5 --chunk-time=1d --batch-size=125"
cat /tmp/bulk_data/timescaledb-data.gz | gunzip | ../../../bin/tsbs_load_timescaledb --do-create-db=true --host="localhost" --db-name="benchmark" --workers=1 --batch-size=125 --field-index="VALUE-TIME" --time-partition-index=true --partitions=5 --chunk-time="24h" --field-index-count="-1"

<<-EOF
"-workers=1 --partitions=5 --chunk-time=1d --batch-size=125
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
1622547498,138246.72,1.382500E+06,138246.72,13824.67,1.382500E+05,13824.67
1622547508,132000.71,2.702500E+06,135123.76,13200.07,2.702500E+05,13512.38
1622547518,125375.62,3.956250E+06,131874.41,12537.56,3.956250E+05,13187.44
1622547528,118625.29,5.142500E+06,128562.15,11862.53,5.142500E+05,12856.21
1622547538,120492.22,6.347500E+06,126948.08,12049.22,6.347500E+05,12694.81
1622547548,118758.24,7.535000E+06,125583.20,11875.82,7.535000E+05,12558.32
1622547558,117124.37,8.706250E+06,124374.79,11712.44,8.706250E+05,12437.48
1622547568,117491.86,9.881250E+06,123514.38,11749.19,9.881250E+05,12351.44

Summary:
loaded 10000000 metrics in 81.059sec with 1 workers (mean rate 123366.46 metrics/sec)
loaded 1000000 rows in 81.059sec with 1 workers (mean rate 12336.65 rows/sec)

select * from hypertable_detailed_size('cpu') ORDER BY node_name;
 table_bytes | index_bytes | toast_bytes | total_bytes | node_name
-------------+-------------+-------------+-------------+-----------
   134438912 |   613785600 |       40960 |   748273664 |
(1 row)
EOF

