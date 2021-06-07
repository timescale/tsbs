#!/usr/bin/env sh
echo "--workers=10 --batch-size=25000"
cat /tmp/bulk_data/clickhouse-data.gz | gunzip | ../../../bin/tsbs_load_clickhouse --user="default" --password="" --do-create-db=true --host="localhost" --db-name="benchmark" --workers=10 --batch-size=25000
:<<-EOF
:<<-EOF
--workers=10 --batch-size=25000
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
1622624229,2974830.76,2.975000E+07,2974830.76,297483.08,2.975000E+06,297483.08
1622624239,4450077.17,7.425000E+07,3712426.59,445007.72,7.425000E+06,371242.66
1622624249,4699650.52,1.212500E+08,4041513.21,469965.05,1.212500E+07,404151.32
1622624259,4575368.65,1.670000E+08,4174965.20,457536.87,1.670000E+07,417496.52
1622624269,4399955.87,2.110000E+08,4219963.40,439995.59,2.110000E+07,421996.34
1622624279,4150035.25,2.525000E+08,4208308.87,415003.52,2.525000E+07,420830.89
1622624289,3995208.76,2.925000E+08,4177834.86,399520.88,2.925000E+07,417783.49
1622624299,3879477.70,3.312500E+08,4140583.64,387947.77,3.312500E+07,414058.36
1622624309,3975164.02,3.710000E+08,4122204.52,397516.40,3.710000E+07,412220.45
1622624319,4100016.33,4.120000E+08,4119985.71,410001.63,4.120000E+07,411998.57
1622624329,4499983.30,4.570000E+08,4154530.96,449998.33,4.570000E+07,415453.10
1622624339,4099956.05,4.980000E+08,4149983.02,409995.61,4.980000E+07,414998.30
1622624349,4175040.62,5.397500E+08,4151910.50,417504.06,5.397500E+07,415191.05
1622624359,3772232.24,5.775000E+08,4124772.22,377223.22,5.775000E+07,412477.22
1622624369,4202618.83,6.195000E+08,4129958.71,420261.88,6.195000E+07,412995.87
1622624379,4450312.68,6.640000E+08,4149979.33,445031.27,6.640000E+07,414997.93
1622624389,4125117.44,7.052500E+08,4148516.91,412511.74,7.052500E+07,414851.69
1622624399,3971798.59,7.450000E+08,4138691.78,397179.86,7.450000E+07,413869.18
1622624409,4003244.82,7.850000E+08,4131568.79,400324.48,7.850000E+07,413156.88
1622624419,4524922.13,8.302500E+08,4151236.73,452492.21,8.302500E+07,415123.67
1622624429,4499826.74,8.752500E+08,4167836.81,449982.67,8.752500E+07,416783.68
1622624439,3775237.78,9.130000E+08,4149992.56,377523.78,9.130000E+07,414999.26
1622624449,4049364.03,9.535000E+08,4145616.76,404936.40,9.535000E+07,414561.68
1622624459,3699258.95,9.905000E+08,4127015.10,369925.89,9.905000E+07,412701.51
1622624469,3750290.50,1.028000E+09,4111947.47,375029.05,1.028000E+08,411194.75
1622624479,4074875.22,1.068750E+09,4110521.59,407487.52,1.068750E+08,411052.16
1622624489,3325761.21,1.102000E+09,4081463.18,332576.12,1.102000E+08,408146.32
1622624499,3438085.45,1.136500E+09,4058408.72,343808.54,1.136500E+08,405840.87
1622624509,3432699.90,1.170750E+09,4036881.95,343269.99,1.170750E+08,403688.20
1622624519,3154115.57,1.202250E+09,4007494.84,315411.56,1.202250E+08,400749.48
1622624529,3399993.61,1.236250E+09,3987898.01,339999.36,1.236250E+08,398789.80
1622624539,3494067.14,1.271250E+09,3972440.44,349406.71,1.271250E+08,397244.04
1622624549,3225480.97,1.303500E+09,3949809.77,322548.10,1.303500E+08,394980.98
1622624559,2853590.04,1.332000E+09,3917608.89,285359.00,1.332000E+08,391760.89
1622624569,3124478.83,1.363250E+09,3894944.57,312447.88,1.363250E+08,389494.46
1622624579,3551534.68,1.398750E+09,3885409.55,355153.47,1.398750E+08,388540.95
1622624589,3174976.91,1.430500E+09,3866208.56,317497.69,1.430500E+08,386620.86
1622624599,2925075.43,1.459750E+09,3841442.57,292507.54,1.459750E+08,384144.26
1622624609,3324938.44,1.493000E+09,3828198.65,332493.84,1.493000E+08,382819.87
1622624619,3288396.09,1.526000E+09,3814657.18,328839.61,1.526000E+08,381465.72
1622624629,3311668.92,1.559000E+09,3802432.43,331166.89,1.559000E+08,380243.24
1622624639,3550069.76,1.594500E+09,3796423.92,355006.98,1.594500E+08,379642.39
1622624649,3449911.40,1.629000E+09,3788365.29,344991.14,1.629000E+08,378836.53
1622624659,2675097.01,1.655750E+09,3763064.68,267509.70,1.655750E+08,376306.47
1622624669,3325016.72,1.689000E+09,3753330.34,332501.67,1.689000E+08,375333.03
1622624679,3323225.73,1.722250E+09,3743975.36,332322.57,1.722250E+08,374397.54
1622624689,3176660.48,1.754000E+09,3731911.16,317666.05,1.754000E+08,373191.12
1622624699,3291905.56,1.787000E+09,3722722.31,329190.56,1.787000E+08,372272.23
1622624709,3107636.16,1.818000E+09,3710200.39,310763.62,1.818000E+08,371020.04
1622624719,2450016.44,1.842500E+09,3684996.90,245001.64,1.842500E+08,368499.69
1622624729,3268123.77,1.875250E+09,3676806.07,326812.38,1.875250E+08,367680.61
1622624739,2780042.30,1.903000E+09,3659592.00,278004.23,1.903000E+08,365959.20

Summary:
loaded 1920000000 metrics in 525.150sec with 10 workers (mean rate 3656101.07 metrics/sec)
loaded 192000000 rows in 525.150sec with 10 workers (mean rate 365610.11 rows/sec)

┌─table──────────────────────────┬─size───────┬──────rows─┬─latest_modification─┬─bytes_size─┬─engine────┬─primary_keys_size─┐
│ benchmark.cpu                  │ 5.55 GiB   │ 192000000 │ 2021-06-02 09:06:54 │ 5955919501 │ MergeTree │ 183.63 KiB        │
│ system.query_log               │ 1.64 MiB   │     15375 │ 2021-06-02 09:25:07 │    1714516 │ MergeTree │ 84.00 B           │
│ system.query_thread_log        │ 1.27 MiB   │      7698 │ 2021-06-02 09:25:06 │    1336357 │ MergeTree │ 66.00 B           │
│ benchmark.tags                 │ 567.87 KiB │     25000 │ 2021-06-02 08:57:00 │     581499 │ MergeTree │ 16.00 B           │
│ system.trace_log               │ 251.25 KiB │      4404 │ 2021-06-02 09:05:51 │     257282 │ MergeTree │ 24.00 B           │
│ system.metric_log              │ 229.03 KiB │      1706 │ 2021-06-02 09:25:24 │     234529 │ MergeTree │ 24.00 B           │
│ system.asynchronous_metric_log │ 8.50 KiB   │      1595 │ 2021-06-02 09:24:58 │       8700 │ MergeTree │ 48.00 B           │
└────────────────────────────────┴────────────┴───────────┴─────────────────────┴────────────┴───────────┴───────────────────┘
EOF
