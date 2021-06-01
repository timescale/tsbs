#!/usr/bin/env sh
echo "--workers=10 --partitions=5 --chunk-time=5m --batch-size=25000"
cat /tmp/bulk_data/timescaledb-data.gz | gunzip | ../../bin/tsbs_load_timescaledb --do-create-db=true --host="localhost" --db-name="benchmark" --workers=10 --batch-size=25000 --field-index="VALUE-TIME" --time-partition-index=true --partitions=5 --chunk-time="5m" --field-index-count="-1"

:<<-EOF
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
1622551774,499992.58,5.000000E+06,499992.58,49999.26,5.000000E+05,49999.26
1622551784,699998.63,1.200000E+07,599994.96,69999.86,1.200000E+06,59999.50
1622551794,874999.33,2.075000E+07,691662.62,87499.93,2.075000E+06,69166.26
1622551804,725000.55,2.800000E+07,699997.06,72500.06,2.800000E+06,69999.71
1622551814,799815.52,3.600000E+07,719964.37,79981.55,3.600000E+06,71996.44
1622551824,850198.48,4.450000E+07,741664.93,85019.85,4.450000E+06,74166.49
1622551834,724982.23,5.175000E+07,739281.65,72498.22,5.175000E+06,73928.16
1622551844,825016.58,6.000000E+07,749998.27,82501.66,6.000000E+06,74999.83
1622551854,749987.90,6.750000E+07,749997.12,74998.79,6.750000E+06,74999.71
1622551864,825011.41,7.575000E+07,757498.43,82501.14,7.575000E+06,75749.84
1622551874,724707.62,8.300000E+07,754516.36,72470.76,8.300000E+06,75451.64
1622551884,700280.54,9.000000E+07,749998.53,70028.05,9.000000E+06,74999.85
1622551894,724985.99,9.725000E+07,748074.45,72498.60,9.725000E+06,74807.45
1622551904,700018.79,1.042500E+08,744642.00,70001.88,1.042500E+07,74464.20
1622551914,774725.41,1.120000E+08,746648.22,77472.54,1.120000E+07,74664.82
1622551924,699995.83,1.190000E+08,743732.50,69999.58,1.190000E+07,74373.25
1622551934,750257.97,1.265000E+08,744116.22,75025.80,1.265000E+07,74411.62
1622551944,800000.24,1.345000E+08,747220.88,80000.02,1.345000E+07,74722.09
1622551954,750007.27,1.420000E+08,747367.53,75000.73,1.420000E+07,74736.75
1622551964,824845.73,1.502500E+08,751242.13,82484.57,1.502500E+07,75124.21
1622551974,724968.00,1.575000E+08,749990.94,72496.80,1.575000E+07,74999.09
1622551984,775183.15,1.652500E+08,751135.77,77518.32,1.652500E+07,75113.58
1622551994,774850.78,1.730000E+08,752167.04,77485.08,1.730000E+07,75216.70
1622552004,699919.59,1.800000E+08,749989.84,69991.96,1.800000E+07,74998.98
1622552014,725083.16,1.872500E+08,748993.70,72508.32,1.872500E+07,74899.37
1622552024,725132.81,1.945000E+08,748076.14,72513.28,1.945000E+07,74807.61
1622552034,774831.63,2.022500E+08,749067.29,77483.16,2.022500E+07,74906.73
1622552044,800172.22,2.102500E+08,750892.07,80017.22,2.102500E+07,75089.21
1622552054,724999.22,2.175000E+08,749999.22,72499.92,2.175000E+07,74999.92
1622552064,825009.09,2.257500E+08,752499.52,82500.91,2.257500E+07,75249.95
1622552074,724999.32,2.330000E+08,751612.41,72499.93,2.330000E+07,75161.24
1622552084,699834.18,2.400000E+08,749993.97,69983.42,2.400000E+07,74999.40
1622552094,725162.54,2.472500E+08,749241.68,72516.25,2.472500E+07,74924.17
1622552104,750013.49,2.547500E+08,749264.38,75001.35,2.547500E+07,74926.44
1622552114,824754.00,2.630000E+08,751421.85,82475.40,2.630000E+07,75142.18
1622552124,725210.89,2.702500E+08,750693.98,72521.09,2.702500E+07,75069.40
1622552134,725000.42,2.775000E+08,749999.56,72500.04,2.775000E+07,74999.96
1622552144,724980.97,2.847500E+08,749341.16,72498.10,2.847500E+07,74934.12
1622552154,824984.57,2.930000E+08,751280.76,82498.46,2.930000E+07,75128.08
1622552164,699837.92,3.000000E+08,749994.40,69983.79,3.000000E+07,74999.44
1622552174,725187.62,3.072500E+08,749389.52,72518.76,3.072500E+07,74938.95
1622552184,775015.96,3.150000E+08,749999.66,77501.60,3.150000E+07,74999.97
1622552194,699866.24,3.220000E+08,748833.55,69986.62,3.220000E+07,74883.35
1622552204,775144.31,3.297500E+08,749431.41,77514.43,3.297500E+07,74943.14
1622552214,775003.85,3.375000E+08,749999.68,77500.39,3.375000E+07,74999.97
1622552224,724988.57,3.447500E+08,749455.95,72498.86,3.447500E+07,74945.60
1622552234,824789.40,3.530000E+08,751059.19,82478.94,3.530000E+07,75105.92
1622552244,699930.64,3.600000E+08,749993.92,69993.06,3.600000E+07,74999.39
1622552254,825307.06,3.682500E+08,751530.35,82530.71,3.682500E+07,75153.03
1622552264,624542.22,3.745000E+08,748988.76,62454.22,3.745000E+07,74898.88
1622552274,850568.06,3.830000E+08,750979.18,85056.81,3.830000E+07,75097.92
1622552284,724832.94,3.902500E+08,750476.25,72483.29,3.902500E+07,75047.63
1622552294,725203.21,3.975000E+08,749999.54,72520.32,3.975000E+07,74999.95
1622552304,724992.45,4.047500E+08,749536.44,72499.25,4.047500E+07,74953.64
1622552314,825021.37,4.130000E+08,750908.86,82502.14,4.130000E+07,75090.89
1622552324,724518.94,4.202500E+08,750437.30,72451.89,4.202500E+07,75043.73
1622552334,700243.87,4.272500E+08,749557.03,70024.39,4.272500E+07,74955.70
1622552344,750147.86,4.347500E+08,749567.21,75014.79,4.347500E+07,74956.72
1622552354,825097.46,4.430000E+08,750847.23,82509.75,4.430000E+07,75084.72
1622552364,724998.16,4.502500E+08,750416.41,72499.82,4.502500E+07,75041.64
1622552374,699976.89,4.572500E+08,749589.51,69997.69,4.572500E+07,74958.95
1622552384,599708.38,4.632500E+08,747170.92,59970.84,4.632500E+07,74717.09
1622552394,825421.95,4.715000E+08,748412.36,82542.19,4.715000E+07,74841.24
1622552404,749863.47,4.790000E+08,748435.04,74986.35,4.790000E+07,74843.50
1622552414,724540.49,4.862500E+08,748067.20,72454.05,4.862500E+07,74806.72
1622552424,725593.43,4.935000E+08,747726.97,72559.34,4.935000E+07,74772.70

Summary:
loaded 500000000 metrics in 668.595sec with 10 workers (mean rate 747837.20 metrics/sec)
loaded 50000000 rows in 668.595sec with 10 workers (mean rate 74783.72 rows/sec)

benchmark=# select * from hypertable_detailed_size('cpu') ORDER BY node_name;
 table_bytes | index_bytes | toast_bytes | total_bytes | node_name
-------------+-------------+-------------+-------------+-----------
  6813908992 | 24439422976 |    17080320 | 31270420480 |
EOF
