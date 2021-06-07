#!/usr/bin/env bash
for f in /tmp/bulk_queries/clickhouse*.gz;
do
	suffix=$(echo "$f" | sed -e "s/^.*clickhouse-//" -e "s/\.gz$//")
	echo "------ Running $suffix"
	echo
	cat $f | gunzip | ../../../bin/tsbs_run_queries_clickhouse --workers=32 --max-queries=200 --debug=0 --print-interval=100 --hdr-latencies="/tmp/bulk_queries/latencies-clickhouse-$suffix.hdr" --burn-in=0 | tee -a /tmp/bulk_queries/clickhouse.log
done

:<<-EOF
Run complete after 1000 queries with 32 workers (Overall query rate 328.35 queries/sec):
ClickHouse max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h:
min:    26.96ms, med:    93.75ms, mean:    93.50ms, max:  185.35ms, stddev:    20.68ms, sum:  93.5sec, count: 1000
all queries                                                              :
min:    26.96ms, med:    93.75ms, mean:    93.50ms, max:  185.35ms, stddev:    20.68ms, sum:  93.5sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-1-queries.hdr
wall clock time: 3.072297sec
Run complete after 1000 queries with 32 workers (Overall query rate 267.00 queries/sec):
ClickHouse max of all CPU metrics, random    8 hosts, random 8h0m0s by 1h:
min:    33.09ms, med:   114.61ms, mean:   118.27ms, max:  297.28ms, stddev:    42.83ms, sum: 118.3sec, count: 1000
all queries                                                              :
min:    33.09ms, med:   114.61ms, mean:   118.27ms, max:  297.28ms, stddev:    42.83ms, sum: 118.3sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-8-queries.hdr
wall clock time: 3.775105sec
Run complete after 1000 queries with 32 workers (Overall query rate 16.59 queries/sec):
ClickHouse mean of 1 metrics, all hosts, random 12h0m0s by 1h:
min:   200.40ms, med:  1113.41ms, mean:  1901.87ms, max: 8116.22ms, stddev:  1769.27ms, sum: 1901.9sec, count: 1000
all queries                                                  :
min:   200.40ms, med:  1113.41ms, mean:  1901.87ms, max: 8116.22ms, stddev:  1769.27ms, sum: 1901.9sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-1-queries.hdr
wall clock time: 60.349588sec
Run complete after 1000 queries with 32 workers (Overall query rate 6.44 queries/sec):
ClickHouse mean of 5 metrics, all hosts, random 12h0m0s by 1h:
min:   514.90ms, med:  1359.87ms, mean:  4861.08ms, max: 23475.20ms, stddev:  7315.45ms, sum: 4861.1sec, count: 1000
all queries                                                  :
min:   514.90ms, med:  1359.87ms, mean:  4861.08ms, max: 23475.20ms, stddev:  7315.45ms, sum: 4861.1sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-5-queries.hdr
wall clock time: 155.306865sec
Run complete after 1000 queries with 32 workers (Overall query rate 3.82 queries/sec):
ClickHouse mean of 10 metrics, all hosts, random 12h0m0s by 1h:
min:   898.46ms, med:  1663.04ms, mean:  8170.30ms, max: 46462.97ms, stddev: 13450.81ms, sum: 8170.3sec, count: 1000
all queries                                                   :
min:   898.46ms, med:  1663.04ms, mean:  8170.30ms, max: 46462.97ms, stddev: 13450.81ms, sum: 8170.3sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-all-queries.hdr
wall clock time: 261.751736sec
Run complete after 1000 queries with 32 workers (Overall query rate 1.57 queries/sec):
ClickHouse max cpu over last 5 min-intervals (random end):
min:  3111.68ms, med: 21143.55ms, mean: 20292.61ms, max: 27086.85ms, stddev:  4120.79ms, sum: 20292.6sec, count: 1000
all queries                                              :
min:  3111.68ms, med: 21143.55ms, mean: 20292.61ms, max: 27086.85ms, stddev:  4120.79ms, sum: 20292.6sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-groupby-orderby-limit-queries.hdr
wall clock time: 637.731397sec
Run complete after 1000 queries with 32 workers (Overall query rate 282.99 queries/sec):
ClickHouse CPU over threshold, 1 host(s):
min:    40.27ms, med:   107.93ms, mean:   111.32ms, max:  343.60ms, stddev:    33.29ms, sum: 111.3sec, count: 1000
all queries                             :
min:    40.27ms, med:   107.93ms, mean:   111.32ms, max:  343.60ms, stddev:    33.29ms, sum: 111.3sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-high-cpu-1-queries.hdr
wall clock time: 3.574579sec
Run complete after 1000 queries with 32 workers (Overall query rate 334.82 queries/sec):
ClickHouse max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h:
min:    25.06ms, med:    92.19ms, mean:    93.80ms, max:  216.91ms, stddev:    22.44ms, sum:  93.8sec, count: 1000
all queries                                                              :
min:    25.06ms, med:    92.19ms, mean:    93.80ms, max:  216.91ms, stddev:    22.44ms, sum:  93.8sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-1-queries.hdr
wall clock time: 3.015611sec
Run complete after 1000 queries with 32 workers (Overall query rate 261.20 queries/sec):
ClickHouse max of all CPU metrics, random    8 hosts, random 8h0m0s by 1h:
min:    33.92ms, med:   116.98ms, mean:   120.57ms, max:  335.12ms, stddev:    41.74ms, sum: 120.6sec, count: 1000
all queries                                                              :
min:    33.92ms, med:   116.98ms, mean:   120.57ms, max:  335.12ms, stddev:    41.74ms, sum: 120.6sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-8-queries.hdr
wall clock time: 3.858921sec
Run complete after 1000 queries with 32 workers (Overall query rate 16.21 queries/sec):
ClickHouse mean of 1 metrics, all hosts, random 12h0m0s by 1h:
min:   426.62ms, med:  1139.33ms, mean:  1947.29ms, max: 7784.96ms, stddev:  1794.63ms, sum: 1947.3sec, count: 1000
all queries                                                  :
min:   426.62ms, med:  1139.33ms, mean:  1947.29ms, max: 7784.96ms, stddev:  1794.63ms, sum: 1947.3sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-1-queries.hdr
wall clock time: 61.773437sec
Run complete after 1000 queries with 32 workers (Overall query rate 6.28 queries/sec):
ClickHouse mean of 5 metrics, all hosts, random 12h0m0s by 1h:
min:   333.78ms, med:  1410.69ms, mean:  4980.92ms, max: 24211.46ms, stddev:  7477.95ms, sum: 4980.9sec, count: 1000
all queries                                                  :
min:   333.78ms, med:  1410.69ms, mean:  4980.92ms, max: 24211.46ms, stddev:  7477.95ms, sum: 4980.9sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-5-queries.hdr
wall clock time: 159.249264sec
Run complete after 1000 queries with 32 workers (Overall query rate 3.73 queries/sec):
ClickHouse mean of 10 metrics, all hosts, random 12h0m0s by 1h:
min:   738.27ms, med:  1718.34ms, mean:  8356.82ms, max: 44115.97ms, stddev: 13690.53ms, sum: 8356.8sec, count: 1000
all queries                                                   :
min:   738.27ms, med:  1718.34ms, mean:  8356.82ms, max: 44115.97ms, stddev: 13690.53ms, sum: 8356.8sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-all-queries.hdr
wall clock time: 268.457711sec
Run complete after 1000 queries with 32 workers (Overall query rate 1.46 queries/sec):
ClickHouse max cpu over last 5 min-intervals (random end):
min:  3315.46ms, med: 22677.50ms, mean: 21855.51ms, max: 30016.51ms, stddev:  4328.13ms, sum: 21855.5sec, count: 1000
all queries                                              :
min:  3315.46ms, med: 22677.50ms, mean: 21855.51ms, max: 30016.51ms, stddev:  4328.13ms, sum: 21855.5sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-groupby-orderby-limit-queries.hdr
wall clock time: 687.360917sec
Run complete after 1000 queries with 32 workers (Overall query rate 219.54 queries/sec):
ClickHouse CPU over threshold, 1 host(s):
min:    43.67ms, med:   139.66ms, mean:   143.70ms, max:  447.02ms, stddev:    47.43ms, sum: 143.7sec, count: 1000
all queries                             :
min:    43.67ms, med:   139.66ms, mean:   143.70ms, max:  447.02ms, stddev:    47.43ms, sum: 143.7sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-high-cpu-1-queries.hdr
wall clock time: 4.605328sec
Run complete after 100 queries with 32 workers (Overall query rate 5.63 queries/sec):
ClickHouse mean of 10 metrics, all hosts, random 12h0m0s by 1h:
min:   825.25ms, med:  1384.64ms, mean:  3479.06ms, max: 17460.22ms, stddev:  4815.18ms, sum: 347.9sec, count: 100
all queries                                                   :
min:   825.25ms, med:  1384.64ms, mean:  3479.06ms, max: 17460.22ms, stddev:  4815.18ms, sum: 347.9sec, count: 100
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-all-queries.hdr
wall clock time: 17.832884sec
Run complete after 1000 queries with 32 workers (Overall query rate 332.69 queries/sec):
ClickHouse max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h:
min:    38.91ms, med:    93.03ms, mean:    95.01ms, max:  312.37ms, stddev:    24.36ms, sum:  95.0sec, count: 1000
all queries                                                              :
min:    38.91ms, med:    93.03ms, mean:    95.01ms, max:  312.37ms, stddev:    24.36ms, sum:  95.0sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-1-queries.hdr
wall clock time: 3.037554sec
Run complete after 1000 queries with 32 workers (Overall query rate 185.26 queries/sec):
ClickHouse max of all CPU metrics, random    8 hosts, random 8h0m0s by 1h:
min:    29.82ms, med:   122.15ms, mean:   171.16ms, max: 1367.74ms, stddev:   167.47ms, sum: 171.2sec, count: 1000
all queries                                                              :
min:    29.82ms, med:   122.15ms, mean:   171.16ms, max: 1367.74ms, stddev:   167.47ms, sum: 171.2sec, count: 1000
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-8-queries.hdr
wall clock time: 5.436871sec
Run complete after 200 queries with 32 workers (Overall query rate 304.29 queries/sec):
ClickHouse max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h:
min:    38.87ms, med:    94.14ms, mean:    99.41ms, max:  403.69ms, stddev:    39.26ms, sum:  19.9sec, count: 200
all queries                                                              :
min:    38.87ms, med:    94.14ms, mean:    99.41ms, max:  403.69ms, stddev:    39.26ms, sum:  19.9sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-1-queries.hdr
wall clock time: 0.685271sec
Run complete after 200 queries with 32 workers (Overall query rate 262.31 queries/sec):
ClickHouse max of all CPU metrics, random    8 hosts, random 8h0m0s by 1h:
min:    31.64ms, med:   109.83ms, mean:   115.39ms, max:  305.79ms, stddev:    42.92ms, sum:  23.1sec, count: 200
all queries                                                              :
min:    31.64ms, med:   109.83ms, mean:   115.39ms, max:  305.79ms, stddev:    42.92ms, sum:  23.1sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-cpu-max-all-8-queries.hdr
wall clock time: 0.789434sec
Run complete after 200 queries with 32 workers (Overall query rate 18.96 queries/sec):
ClickHouse mean of 1 metrics, all hosts, random 12h0m0s by 1h:
min:   459.90ms, med:   957.98ms, mean:  1538.50ms, max: 5947.90ms, stddev:  1331.64ms, sum: 307.7sec, count: 200
all queries                                                  :
min:   459.90ms, med:   957.98ms, mean:  1538.50ms, max: 5947.90ms, stddev:  1331.64ms, sum: 307.7sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-1-queries.hdr
wall clock time: 10.598625sec
Run complete after 200 queries with 32 workers (Overall query rate 7.67 queries/sec):
ClickHouse mean of 5 metrics, all hosts, random 12h0m0s by 1h:
min:   541.79ms, med:  1239.30ms, mean:  3741.61ms, max: 20295.68ms, stddev:  5645.59ms, sum: 748.3sec, count: 200
all queries                                                  :
min:   541.79ms, med:  1239.30ms, mean:  3741.61ms, max: 20295.68ms, stddev:  5645.59ms, sum: 748.3sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-5-queries.hdr
wall clock time: 26.126223sec
Run complete after 200 queries with 32 workers (Overall query rate 4.63 queries/sec):
ClickHouse mean of 10 metrics, all hosts, random 12h0m0s by 1h:
min:   727.65ms, med:  1498.69ms, mean:  6267.25ms, max: 33732.61ms, stddev: 10110.89ms, sum: 1253.4sec, count: 200
all queries                                                   :
min:   727.65ms, med:  1498.69ms, mean:  6267.25ms, max: 33732.61ms, stddev: 10110.89ms, sum: 1253.4sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-double-groupby-all-queries.hdr
wall clock time: 43.280925sec
Run complete after 200 queries with 32 workers (Overall query rate 1.73 queries/sec):
ClickHouse max cpu over last 5 min-intervals (random end):
min:  3384.45ms, med: 19107.84ms, mean: 17989.24ms, max: 24096.77ms, stddev:  3903.07ms, sum: 3597.8sec, count: 200
all queries                                              :
min:  3384.45ms, med: 19107.84ms, mean: 17989.24ms, max: 24096.77ms, stddev:  3903.07ms, sum: 3597.8sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-groupby-orderby-limit-queries.hdr
wall clock time: 115.350300sec
Run complete after 200 queries with 32 workers (Overall query rate 352.24 queries/sec):
ClickHouse CPU over threshold, 1 host(s):
min:    27.81ms, med:    82.01ms, mean:    85.31ms, max:  213.82ms, stddev:    27.93ms, sum:  17.1sec, count: 200
all queries                             :
min:    27.81ms, med:    82.01ms, mean:    85.31ms, max:  213.82ms, stddev:    27.93ms, sum:  17.1sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-high-cpu-1-queries.hdr
wall clock time: 0.594469sec
Run complete after 200 queries with 32 workers (Overall query rate 1.95 queries/sec):
ClickHouse CPU over threshold, all hosts:
min:  3485.18ms, med: 12292.09ms, mean: 16334.44ms, max: 65912.83ms, stddev: 14711.43ms, sum: 3266.9sec, count: 200
all queries                             :
min:  3485.18ms, med: 12292.09ms, mean: 16334.44ms, max: 65912.83ms, stddev: 14711.43ms, sum: 3266.9sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-high-cpu-all-queries.hdr
wall clock time: 102.710961sec
Run complete after 200 queries with 32 workers (Overall query rate 2.87 queries/sec):
ClickHouse last row per host:
min:  4919.81ms, med: 12268.03ms, mean: 10835.96ms, max: 14327.30ms, stddev:  2730.70ms, sum: 2167.2sec, count: 200
all queries                 :
min:  4919.81ms, med: 12268.03ms, mean: 10835.96ms, max: 14327.30ms, stddev:  2730.70ms, sum: 2167.2sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-lastpoint-queries.hdr
wall clock time: 69.872026sec
Run complete after 200 queries with 32 workers (Overall query rate 329.99 queries/sec):
ClickHouse 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m:
min:    32.96ms, med:    90.47ms, mean:    89.42ms, max:  143.60ms, stddev:    17.55ms, sum:  17.9sec, count: 200
all queries                                                       :
min:    32.96ms, med:    90.47ms, mean:    89.42ms, max:  143.60ms, stddev:    17.55ms, sum:  17.9sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-single-groupby-1-1-1-queries.hdr
wall clock time: 0.633711sec
Run complete after 200 queries with 32 workers (Overall query rate 340.02 queries/sec):
ClickHouse 1 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:    31.80ms, med:    86.19ms, mean:    87.78ms, max:  168.42ms, stddev:    23.16ms, sum:  17.6sec, count: 200
all queries                                                        :
min:    31.80ms, med:    86.19ms, mean:    87.78ms, max:  168.42ms, stddev:    23.16ms, sum:  17.6sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-single-groupby-1-1-12-queries.hdr
wall clock time: 0.616488sec
Run complete after 200 queries with 32 workers (Overall query rate 287.65 queries/sec):
ClickHouse 1 cpu metric(s), random    8 hosts, random 1h0m0s by 1m:
min:    20.72ms, med:   102.92ms, mean:   103.80ms, max:  220.54ms, stddev:    28.17ms, sum:  20.8sec, count: 200
all queries                                                       :
min:    20.72ms, med:   102.92ms, mean:   103.80ms, max:  220.54ms, stddev:    28.17ms, sum:  20.8sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-single-groupby-1-8-1-queries.hdr
wall clock time: 0.726299sec
Run complete after 200 queries with 32 workers (Overall query rate 310.50 queries/sec):
ClickHouse 5 cpu metric(s), random    1 hosts, random 1h0m0s by 1m:
min:    27.04ms, med:    94.03ms, mean:    96.37ms, max:  169.97ms, stddev:    22.70ms, sum:  19.3sec, count: 200
all queries                                                       :
min:    27.04ms, med:    94.03ms, mean:    96.37ms, max:  169.97ms, stddev:    22.70ms, sum:  19.3sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-single-groupby-5-1-1-queries.hdr
wall clock time: 0.672544sec
Run complete after 200 queries with 32 workers (Overall query rate 283.74 queries/sec):
ClickHouse 5 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:    31.00ms, med:   105.56ms, mean:   105.85ms, max:  177.92ms, stddev:    27.76ms, sum:  21.2sec, count: 200
all queries                                                        :
min:    31.00ms, med:   105.56ms, mean:   105.85ms, max:  177.92ms, stddev:    27.76ms, sum:  21.2sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-single-groupby-5-1-12-queries.hdr
wall clock time: 0.733528sec
Run complete after 200 queries with 32 workers (Overall query rate 253.03 queries/sec):
ClickHouse 5 cpu metric(s), random    8 hosts, random 1h0m0s by 1m:
min:    40.23ms, med:   114.14ms, mean:   118.52ms, max:  217.41ms, stddev:    31.41ms, sum:  23.7sec, count: 200
all queries                                                       :
min:    40.23ms, med:   114.14ms, mean:   118.52ms, max:  217.41ms, stddev:    31.41ms, sum:  23.7sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-clickhouse-single-groupby-5-8-1-queries.hdr
wall clock time: 0.821378sec
EOF
