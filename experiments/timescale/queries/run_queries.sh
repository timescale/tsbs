#!/usr/bin/env bash
for f in /tmp/bulk_queries/timescaledb*.gz;
do
	suffix=$(echo "$f" | sed -e "s/^.*timescaledb-//" -e "s/\.gz$//")
	echo "------ Running $suffix"
	echo
	cat $f | gunzip | ../../../bin/tsbs_run_queries_timescaledb --workers=32 --max-queries=200 --debug=0 --print-interval=100 --hdr-latencies="/tmp/bulk_queries/latencies-timescaledb-$suffix.hdr" --burn-in=0 | tee -a /tmp/bulk_queries/timescale.log
done

:<<-EOF
 ------ Running cpu-max-all-1-queries

After 100 queries with 32 workers:
Interval query rate: 32.71 queries/sec	Overall query rate: 32.71 queries/sec
TimescaleDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h:
min:     1.96ms, med:     3.23ms, mean:    50.61ms, max: 3060.22ms, stddev:   304.56ms, sum:   5.1sec, count: 100
all queries                                                               :
min:     1.96ms, med:     3.23ms, mean:    50.61ms, max: 3060.22ms, stddev:   304.56ms, sum:   5.1sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 10.44 queries/sec):
TimescaleDB max of all CPU metrics, random    1 hosts, random 8h0m0s by 1h:
min:     1.96ms, med:     3.35ms, mean:  2677.30ms, max: 19078.14ms, stddev:  5313.79ms, sum: 535.5sec, count: 200
all queries                                                               :
min:     1.96ms, med:     3.35ms, mean:  2677.30ms, max: 19078.14ms, stddev:  5313.79ms, sum: 535.5sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-cpu-max-all-1-queries.hdr
wall clock time: 19.217659sec
------ Running cpu-max-all-8-queries

After 100 queries with 32 workers:
Interval query rate: 661.93 queries/sec	Overall query rate: 661.93 queries/sec
TimescaleDB max of all CPU metrics, random    8 hosts, random 8h0m0s by 1h:
min:     2.38ms, med:     3.55ms, mean:    26.72ms, max:  149.43ms, stddev:    45.00ms, sum:   2.7sec, count: 100
all queries                                                               :
min:     2.38ms, med:     3.55ms, mean:    26.72ms, max:  149.43ms, stddev:    45.00ms, sum:   2.7sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 2.19 queries/sec):
TimescaleDB max of all CPU metrics, random    8 hosts, random 8h0m0s by 1h:
min:     2.09ms, med:     3.71ms, mean: 13608.88ms, max: 90791.93ms, stddev: 30075.33ms, sum: 2721.8sec, count: 200
all queries                                                               :
min:     2.09ms, med:     3.71ms, mean: 13608.88ms, max: 90791.93ms, stddev: 30075.33ms, sum: 2721.8sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-cpu-max-all-8-queries.hdr
wall clock time: 91.436289sec
------ Running double-groupby-1-queries

After 100 queries with 32 workers:
Interval query rate: 411.87 queries/sec	Overall query rate: 411.87 queries/sec
TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h:
min:     2.57ms, med:     8.73ms, mean:    59.57ms, max:  227.22ms, stddev:    83.98ms, sum:   6.0sec, count: 100
all queries                                                   :
min:     2.57ms, med:     8.73ms, mean:    59.57ms, max:  227.22ms, stddev:    83.98ms, sum:   6.0sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 0.62 queries/sec):
TimescaleDB mean of 1 metrics, all hosts, random 12h0m0s by 1h:
min:     2.06ms, med:     9.69ms, mean: 43495.08ms, max: 320487.42ms, stddev: 97185.40ms, sum: 8699.0sec, count: 200
all queries                                                   :
min:     2.06ms, med:     9.69ms, mean: 43495.08ms, max: 320487.42ms, stddev: 97185.40ms, sum: 8699.0sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-double-groupby-1-queries.hdr
wall clock time: 320.825414sec
------ Running double-groupby-5-queries

After 100 queries with 32 workers:
Interval query rate: 303.80 queries/sec	Overall query rate: 303.80 queries/sec
TimescaleDB mean of 5 metrics, all hosts, random 12h0m0s by 1h:
min:     2.91ms, med:     9.32ms, mean:    80.56ms, max:  316.72ms, stddev:   113.40ms, sum:   8.1sec, count: 100
all queries                                                   :
min:     2.91ms, med:     9.32ms, mean:    80.56ms, max:  316.72ms, stddev:   113.40ms, sum:   8.1sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 0.38 queries/sec):
TimescaleDB mean of 5 metrics, all hosts, random 12h0m0s by 1h:
min:     2.06ms, med:    10.49ms, mean: 76151.74ms, max: 530153.47ms, stddev: 170997.43ms, sum: 15230.3sec, count: 200
all queries                                                   :
min:     2.06ms, med:    10.49ms, mean: 76151.74ms, max: 530153.47ms, stddev: 170997.43ms, sum: 15230.3sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-double-groupby-5-queries.hdr
wall clock time: 530.561990sec
------ Running double-groupby-all-queries

After 100 queries with 32 workers:
Interval query rate: 293.36 queries/sec	Overall query rate: 293.36 queries/sec
TimescaleDB mean of 10 metrics, all hosts, random 12h0m0s by 1h:
min:     3.22ms, med:    14.99ms, mean:    86.68ms, max:  291.87ms, stddev:   119.46ms, sum:   8.7sec, count: 100
all queries                                                    :
min:     3.22ms, med:    14.99ms, mean:    86.68ms, max:  291.87ms, stddev:   119.46ms, sum:   8.7sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 0.23 queries/sec):
TimescaleDB mean of 10 metrics, all hosts, random 12h0m0s by 1h:
min:     2.32ms, med:    15.25ms, mean: 124476.67ms, max: 861143.04ms, stddev: 279750.78ms, sum: 24895.3sec, count: 200
all queries                                                    :
min:     2.32ms, med:    15.25ms, mean: 124476.67ms, max: 861143.04ms, stddev: 279750.78ms, sum: 24895.3sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-double-groupby-all-queries.hdr
wall clock time: 861.602530sec
------ Running groupby-orderby-limit-queries

panic: ERROR: out of shared memory (SQLSTATE 53200)

goroutine 50 [running]:
github.com/timescale/tsbs/pkg/query.(*BenchmarkRunner).processorHandler(0xc0000f4420, 0xc0000b2df0, 0xc0000e6870, 0x186a400, 0x15eb560, 0xc0001db3f0, 0x1e)
	/Users/miguel/Code/timescale/tsbs/pkg/query/benchmarker.go:196 +0x29b
created by github.com/timescale/tsbs/pkg/query.(*BenchmarkRunner).Run
	/Users/miguel/Code/timescale/tsbs/pkg/query/benchmarker.go:156 +0x205
------ Running high-cpu-1-queries

After 100 queries with 32 workers:
Interval query rate: 431.85 queries/sec	Overall query rate: 431.85 queries/sec
TimescaleDB CPU over threshold, 1 host(s):
min:     2.54ms, med:     5.38ms, mean:    48.79ms, max:  222.96ms, stddev:    74.89ms, sum:   4.9sec, count: 100
all queries                              :
min:     2.54ms, med:     5.38ms, mean:    48.79ms, max:  222.96ms, stddev:    74.89ms, sum:   4.9sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 8.35 queries/sec):
TimescaleDB CPU over threshold, 1 host(s):
min:     1.76ms, med:     4.96ms, mean:  3325.86ms, max: 23938.05ms, stddev:  7293.71ms, sum: 665.2sec, count: 200
all queries                              :
min:     1.76ms, med:     4.96ms, mean:  3325.86ms, max: 23938.05ms, stddev:  7293.71ms, sum: 665.2sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-high-cpu-1-queries.hdr
wall clock time: 23.995647sec
------ Running high-cpu-all-queries

After 100 queries with 32 workers:
Interval query rate: 642.17 queries/sec	Overall query rate: 642.17 queries/sec
TimescaleDB CPU over threshold, all hosts:
min:     1.73ms, med:     3.66ms, mean:    29.17ms, max:  152.19ms, stddev:    46.29ms, sum:   2.9sec, count: 100
all queries                              :
min:     1.73ms, med:     3.66ms, mean:    29.17ms, max:  152.19ms, stddev:    46.29ms, sum:   2.9sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 0.99 queries/sec):
TimescaleDB CPU over threshold, all hosts:
min:     1.73ms, med:     4.73ms, mean: 26414.13ms, max: 168329.21ms, stddev: 56728.47ms, sum: 5282.8sec, count: 200
all queries                              :
min:     1.73ms, med:     4.73ms, mean: 26414.13ms, max: 168329.21ms, stddev: 56728.47ms, sum: 5282.8sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-high-cpu-all-queries.hdr
wall clock time: 201.918843sec
------ Running lastpoint-queries

panic: ERROR: out of shared memory (SQLSTATE 53200)

goroutine 51 [running]:
github.com/timescale/tsbs/pkg/query.(*BenchmarkRunner).processorHandler(0xc000184790, 0xc0000c4df0, 0xc0000ae8c0, 0x186a400, 0x15eb560, 0xc0001ed400, 0x1f)
	/Users/miguel/Code/timescale/tsbs/pkg/query/benchmarker.go:196 +0x29b
created by github.com/timescale/tsbs/pkg/query.(*BenchmarkRunner).Run
	/Users/miguel/Code/timescale/tsbs/pkg/query/benchmarker.go:156 +0x205
------ Running single-groupby-1-1-1-queries

After 100 queries with 32 workers:
Interval query rate: 551.31 queries/sec	Overall query rate: 551.31 queries/sec
TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m:
min:     2.35ms, med:     5.33ms, mean:    30.92ms, max:  174.16ms, stddev:    50.81ms, sum:   3.1sec, count: 100
all queries                                                        :
min:     2.35ms, med:     5.33ms, mean:    30.92ms, max:  174.16ms, stddev:    50.81ms, sum:   3.1sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 113.93 queries/sec):
TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m:
min:     2.35ms, med:     5.73ms, mean:   239.81ms, max: 1711.87ms, stddev:   515.90ms, sum:  48.0sec, count: 200
all queries                                                        :
min:     2.35ms, med:     5.73ms, mean:   239.81ms, max: 1711.87ms, stddev:   515.90ms, sum:  48.0sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-single-groupby-1-1-1-queries.hdr
wall clock time: 1.798388sec
------ Running single-groupby-1-1-12-queries

After 100 queries with 32 workers:
Interval query rate: 634.29 queries/sec	Overall query rate: 634.29 queries/sec
TimescaleDB 1 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:     2.28ms, med:     4.63ms, mean:    29.93ms, max:  154.69ms, stddev:    47.28ms, sum:   3.0sec, count: 100
all queries                                                         :
min:     2.28ms, med:     4.63ms, mean:    29.93ms, max:  154.69ms, stddev:    47.28ms, sum:   3.0sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 13.29 queries/sec):
TimescaleDB 1 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:     2.11ms, med:     5.68ms, mean:  2098.34ms, max: 14660.09ms, stddev:  4266.58ms, sum: 419.7sec, count: 200
all queries                                                         :
min:     2.11ms, med:     5.68ms, mean:  2098.34ms, max: 14660.09ms, stddev:  4266.58ms, sum: 419.7sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-single-groupby-1-1-12-queries.hdr
wall clock time: 15.105685sec
------ Running single-groupby-1-8-1-queries

After 100 queries with 32 workers:
Interval query rate: 452.45 queries/sec	Overall query rate: 452.45 queries/sec
TimescaleDB 1 cpu metric(s), random    8 hosts, random 1h0m0s by 1m:
min:     2.65ms, med:     5.40ms, mean:    34.80ms, max:  218.46ms, stddev:    61.10ms, sum:   3.5sec, count: 100
all queries                                                        :
min:     2.65ms, med:     5.40ms, mean:    34.80ms, max:  218.46ms, stddev:    61.10ms, sum:   3.5sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 27.68 queries/sec):
TimescaleDB 1 cpu metric(s), random    8 hosts, random 1h0m0s by 1m:
min:     2.52ms, med:     6.53ms, mean:   870.77ms, max: 7220.22ms, stddev:  2125.39ms, sum: 174.2sec, count: 200
all queries                                                        :
min:     2.52ms, med:     6.53ms, mean:   870.77ms, max: 7220.22ms, stddev:  2125.39ms, sum: 174.2sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-single-groupby-1-8-1-queries.hdr
wall clock time: 7.271460sec
------ Running single-groupby-5-1-1-queries

After 100 queries with 32 workers:
Interval query rate: 679.72 queries/sec	Overall query rate: 679.72 queries/sec
TimescaleDB 5 cpu metric(s), random    1 hosts, random 1h0m0s by 1m:
min:     2.56ms, med:     5.40ms, mean:    25.67ms, max:  137.40ms, stddev:    39.62ms, sum:   2.6sec, count: 100
all queries                                                        :
min:     2.56ms, med:     5.40ms, mean:    25.67ms, max:  137.40ms, stddev:    39.62ms, sum:   2.6sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 730.40 queries/sec):
TimescaleDB 5 cpu metric(s), random    1 hosts, random 1h0m0s by 1m:
min:     2.56ms, med:     5.84ms, mean:    37.95ms, max:  232.35ms, stddev:    56.01ms, sum:   7.6sec, count: 200
all queries                                                        :
min:     2.56ms, med:     5.84ms, mean:    37.95ms, max:  232.35ms, stddev:    56.01ms, sum:   7.6sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-single-groupby-5-1-1-queries.hdr
wall clock time: 0.298269sec
------ Running single-groupby-5-1-12-queries

After 100 queries with 32 workers:
Interval query rate: 663.85 queries/sec	Overall query rate: 663.85 queries/sec
TimescaleDB 5 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:     2.37ms, med:     6.66ms, mean:    32.86ms, max:  145.78ms, stddev:    45.55ms, sum:   3.3sec, count: 100
all queries                                                         :
min:     2.37ms, med:     6.66ms, mean:    32.86ms, max:  145.78ms, stddev:    45.55ms, sum:   3.3sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 250.97 queries/sec):
TimescaleDB 5 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:     2.37ms, med:     7.97ms, mean:   112.73ms, max:  699.93ms, stddev:   182.88ms, sum:  22.5sec, count: 200
all queries                                                         :
min:     2.37ms, med:     7.97ms, mean:   112.73ms, max:  699.93ms, stddev:   182.88ms, sum:  22.5sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-single-groupby-5-1-12-queries.hdr
wall clock time: 0.829171sec
------ Running single-groupby-5-8-1-queries

After 100 queries with 32 workers:
Interval query rate: 614.59 queries/sec	Overall query rate: 614.59 queries/sec
TimescaleDB 5 cpu metric(s), random    8 hosts, random 1h0m0s by 1m:
min:     2.50ms, med:     5.96ms, mean:    33.47ms, max:  160.31ms, stddev:    51.78ms, sum:   3.3sec, count: 100
all queries                                                        :
min:     2.50ms, med:     5.96ms, mean:    33.47ms, max:  160.31ms, stddev:    51.78ms, sum:   3.3sec, count: 100

Run complete after 200 queries with 32 workers (Overall query rate 532.68 queries/sec):
TimescaleDB 5 cpu metric(s), random    8 hosts, random 1h0m0s by 1m:
min:     2.50ms, med:     6.38ms, mean:    48.52ms, max:  362.30ms, stddev:    77.67ms, sum:   9.7sec, count: 200
all queries                                                        :
min:     2.50ms, med:     6.38ms, mean:    48.52ms, max:  362.30ms, stddev:    77.67ms, sum:   9.7sec, count: 200
Saving High Dynamic Range (HDR) Histogram of Response Latencies to /tmp/bulk_queries/latencies-timescaledb-single-groupby-5-8-1-queries.hdr
wall clock time: 0.405099sec
EOF

compressed=<<-EOF

EOF
