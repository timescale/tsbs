#!/usr/bin/env sh
echo "--workers=10 --partitions=5 --chunk-time=5m --batch-size=2500"
cat /tmp/bulk_data/timescaledb-data.gz | gunzip | ../../bin/tsbs_load_timescaledb --do-create-db=true --host="localhost" --db-name="benchmark" --workers=10 --batch-size=2500 --field-index="VALUE-TIME" --time-partition-index=true --partitions=5 --chunk-time="5m" --field-index-count="-1"

:<<-EOF
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
1622550379,904972.04,9.050000E+06,904972.04,90497.20,9.050000E+05,90497.20
1622550389,1002498.80,1.907500E+07,953734.69,100249.88,1.907500E+06,95373.47
1622550399,999932.00,2.907500E+07,969134.33,99993.20,2.907500E+06,96913.43
1622550409,817534.01,3.725000E+07,931236.38,81753.40,3.725000E+06,93123.64
1622550419,957496.34,4.682500E+07,936488.33,95749.63,4.682500E+06,93648.83
1622550429,937525.14,5.620000E+07,936661.12,93752.51,5.620000E+06,93666.11
1622550439,932478.84,6.552500E+07,936063.65,93247.88,6.552500E+06,93606.36
1622550449,915024.49,7.467500E+07,933433.83,91502.45,7.467500E+06,93343.38
1622550459,964994.52,8.432500E+07,936940.58,96499.45,8.432500E+06,93694.06
1622550469,930001.03,9.362500E+07,936246.63,93000.10,9.362500E+06,93624.66
1622550479,939995.13,1.030250E+08,936587.40,93999.51,1.030250E+07,93658.74
1622550489,950001.97,1.125250E+08,937705.28,95000.20,1.125250E+07,93770.53
1622550499,927490.17,1.218000E+08,936919.49,92749.02,1.218000E+07,93691.95
1622550509,902511.73,1.308250E+08,934461.84,90251.17,1.308250E+07,93446.18
1622550519,885003.09,1.396750E+08,931164.61,88500.31,1.396750E+07,93116.46
1622550530,792172.42,1.476000E+08,922474.24,79217.24,1.476000E+07,92247.42
1622550539,905366.68,1.566500E+08,921468.33,90536.67,1.566500E+07,92146.83
1622550549,897421.89,1.656250E+08,920132.31,89742.19,1.656250E+07,92013.23
1622550559,897578.39,1.746000E+08,918945.36,89757.84,1.746000E+07,91894.54
1622550569,857508.19,1.831750E+08,915873.54,85750.82,1.831750E+07,91587.35
1622550579,879998.39,1.919750E+08,914165.20,87999.84,1.919750E+07,91416.52
1622550590,844580.37,2.004250E+08,911000.76,84458.04,2.004250E+07,91100.08
1622550599,875380.41,2.091750E+08,909452.73,87538.04,2.091750E+07,90945.27
1622550609,910045.03,2.182750E+08,909477.40,91004.50,2.182750E+07,90947.74
1622550619,885013.89,2.271250E+08,908498.88,88501.39,2.271250E+07,90849.89
1622550629,844996.83,2.355750E+08,906056.49,84499.68,2.355750E+07,90605.65
1622550639,805006.06,2.436250E+08,902313.91,80500.61,2.436250E+07,90231.39
1622550650,819744.16,2.518250E+08,899364.11,81974.42,2.518250E+07,89936.41
1622550659,752714.28,2.593500E+08,894308.67,75271.43,2.593500E+07,89430.87
1622550669,902317.13,2.683750E+08,894575.67,90231.71,2.683750E+07,89457.57
1622550679,902461.28,2.774000E+08,894830.05,90246.13,2.774000E+07,89483.00
1622550689,872733.73,2.861250E+08,894139.73,87273.37,2.861250E+07,89413.97
1622550699,862497.92,2.947500E+08,893180.88,86249.79,2.947500E+07,89318.09
1622550709,884998.69,3.036000E+08,892940.23,88499.87,3.036000E+07,89294.02
1622550719,842504.91,3.120250E+08,891499.23,84250.49,3.120250E+07,89149.92
1622550729,889976.16,3.209250E+08,891456.92,88997.62,3.209250E+07,89145.69
1622550739,864992.42,3.295750E+08,890741.66,86499.24,3.295750E+07,89074.17
1622550749,875012.48,3.383250E+08,890327.74,87501.25,3.383250E+07,89032.77
1622550759,849974.31,3.468250E+08,889293.01,84997.43,3.468250E+07,88929.30
1622550769,857516.66,3.554000E+08,888498.62,85751.67,3.554000E+07,88849.86
1622550780,752310.59,3.629250E+08,885176.15,75231.06,3.629250E+07,88517.61
1622550790,864868.70,3.715750E+08,884692.57,86486.87,3.715750E+07,88469.26
1622550799,815354.97,3.797250E+08,883080.77,81535.50,3.797250E+07,88308.08
1622550809,839985.23,3.881250E+08,882101.31,83998.52,3.881250E+07,88210.13
1622550819,859995.85,3.967250E+08,881610.07,85999.58,3.967250E+07,88161.01
1622550829,870013.08,4.054250E+08,881357.97,87001.31,4.054250E+07,88135.80
1622550839,849993.49,4.139250E+08,880690.63,84999.35,4.139250E+07,88069.06
1622550849,842474.26,4.223500E+08,879894.44,84247.43,4.223500E+07,87989.44
1622550859,847532.91,4.308250E+08,879234.02,84753.29,4.308250E+07,87923.40
1622550869,839960.07,4.392250E+08,878448.51,83996.01,4.392250E+07,87844.85
1622550879,840044.21,4.476250E+08,877695.52,84004.42,4.476250E+07,87769.55
1622550889,797501.00,4.556000E+08,876153.32,79750.10,4.556000E+07,87615.33
1622550900,642322.76,4.620250E+08,871740.23,64232.28,4.620250E+07,87174.02
1622550909,800194.59,4.700250E+08,870415.64,80019.46,4.700250E+07,87041.56
1622550919,817524.12,4.782000E+08,869454.00,81752.41,4.782000E+07,86945.40
1622550929,812482.77,4.863250E+08,868436.64,81248.28,4.863250E+07,86843.66
1622550939,810003.09,4.944250E+08,867411.49,81000.31,4.944250E+07,86741.15

Summary:
loaded 500000000 metrics in 576.385sec with 10 workers (mean rate 867475.38 metrics/sec)
loaded 50000000 rows in 576.385sec with 10 workers (mean rate 86747.54 rows/sec)

select * from hypertable_detailed_size('cpu') ORDER BY node_name;
 table_bytes | index_bytes | toast_bytes | total_bytes | node_name
-------------+-------------+-------------+-------------+-----------
  7067615232 | 24154750976 |    17080320 | 31239454720 |
(1 row)
EOF
