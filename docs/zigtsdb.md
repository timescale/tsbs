# TSBS Supplemental Guide: ZigTSDB

ZigTSDB is a time-series database written in Zig with an HTTP line-protocol
ingest path and its own query language, ZQL. It is distributed as a public
container image, `ghcr.io/samisoftb/zigtsdb`. This supplemental guide explains how the data
generated for TSBS is stored, the additional flags of the data importer
(`tsbs_load_zigtsdb`) and of the query runner (`tsbs_run_queries_zigtsdb`),
and how the TSBS devops queries map onto ZQL.

**This should be read *after* the main README.**

## Data model mapping

ZigTSDB stores **one float64 value per series**, where a series is a metric
name plus an ordered set of string labels. A TSBS `cpu` reading carries ten
fields, so every reading is loaded as ten series:

```
cpu,hostname=host_0,region=eu-west-1,... usage_user=58.1,usage_system=2.5,... 1451606400000000000
```

becomes

```
cpu_usage_user,hostname=host_0,region=eu-west-1,... value=58.1 1451606400000000000
cpu_usage_system,hostname=host_0,region=eu-west-1,... value=2.5 1451606400000000000
...
```

- Metric name = `<measurement>_<field>`.
- All TSBS tags are kept as labels, in the order TSBS emits them (ZigTSDB
  derives the series identity from the ordered label list; TSBS emits the
  same order for every reading of a host).
- Integer fields lose the InfluxDB `i` suffix; values are float64.
- String and boolean fields are rejected, so only the `cpu-only` and
  `devops` use cases can be loaded (`iot` has string fields).

TSBS reports "rows" as TSBS readings and "metrics" as fields, i.e. as
individual ZigTSDB points.

## Data format

`tsbs_generate_data --format zigtsdb` emits exactly the InfluxDB
line-protocol bytes of `--format influx`; the expansion above happens in the
loader. One generated file can therefore be loaded into InfluxDB, QuestDB,
VictoriaMetrics and ZigTSDB for an apples-to-apples comparison.

## Starting a server

Pin the image by digest so a benchmark is reproducible:

```bash
docker pull ghcr.io/samisoftb/zigtsdb:1.2.0   # sha256:1ce7cf83f18920367a8a81055bd741c9c1cf8f2d046a2c79e42dac49944787f6
DIGEST=$(docker inspect --format '{{index .RepoDigests 0}}' ghcr.io/samisoftb/zigtsdb:1.2.0)
docker run -d --name zigtsdb -p 8080:8080 \
  -e TSDB_EXPECTED_SERIES=40000 \
  "$DIGEST"
curl http://localhost:8080/health
```

`TSDB_EXPECTED_SERIES` pre-sizes the series map: use `scale × 10` for
`cpu-only` (10 fields per host).

`TSDB_HTTP_THREADS` matters for concurrency: ZigTSDB serves one connection
per HTTP thread and defaults to `min(cpu, 8)`. Every loader and query worker
holds one keep-alive connection, so set it to at least the number of workers
you will run (e.g. `-e TSDB_HTTP_THREADS=32`); a client beyond the thread
count waits for an idle connection to expire (about 30 s) instead of failing.

**Licensing.** A server without a license key runs in the free tier and
meters ingest at 1,000,000 points per day, after which ingest answers
`429 Quota exceeded`. Any TSBS run above that needs a key; activate one with

```bash
curl -X POST http://localhost:8080/license/activate \
  -H 'Content-Type: application/json' -d '{"key":"<key>"}'
```

or export `ZIGTSDB_LICENSE_KEY` before running `scripts/load/load_zigtsdb.sh`.

## `tsbs_load_zigtsdb` additional flags

**`--urls`** (type: `string`, default: `http://localhost:8080`)

Comma-separated node URLs; workers pick one round-robin.

**`--ingest-path`** (type: `string`, default: `/dispatch/ingest/simd?precision=ns`)

Ingest endpoint appended to each URL. The default is the SIMD line parser
with nanosecond timestamps and no auth gate. Use `/ingest` for the
authenticated, replicated path of an HA cluster.

**`--max-body-bytes`** (type: `int`, default: `1900000`)

Maximum bytes per POST. ZigTSDB caps request bodies at 2 MiB, so each TSBS
batch (`--batch-size` readings × 10 lines) is split into several POSTs on
line boundaries. Lower it only if the server answers `413`.

**`--max-retries`** (type: `int`, default: `600`)

Retries per POST when the server answers `429` (quota, ingest backlog,
memory pressure — the server's `retry_after_seconds` is honoured) or `503`
(HA leadership transition, exponential back-off up to 2 s). Retries are
safe: duplicate `(series, timestamp, value)` points are deduplicated.

**`--do-create-db`** (TSBS common flag, default: `true`)

ZigTSDB has no database abstraction. With this flag the loader deletes any
existing metric whose name starts with a TSBS devops measurement (`cpu_`,
`mem_`, `disk_`, `diskio_`, `kernel_`, `net_`, `nginx_`, `postgresl_`,
`redis_`) before loading. Other metrics are never touched.

The unified `tsbs_load` also works; see `docs/sample-configs` for the YAML
shape (target-specific keys: `urls`, `ingest-path`, `max-body-bytes`,
`max-retries`).

## `tsbs_run_queries_zigtsdb` additional flags

**`--urls`** (type: `string`, default: `http://localhost:8080`)

Comma-separated node URLs, round-robin per worker.

**`--fmt-full`** (type: `bool`, default: `false`)

Append `fmt=full` to every request so results carry full float precision.
Use it when validating answers; leave it off when measuring latency.

**`--consistency`** (type: `string`, default: `local`)

Read consistency on an HA cluster: `local`, `linearizable` or `bounded`.

**`--max-retries`** (type: `int`, default: `60`)

Retries per statement when the server answers `429 query_capacity`.

## Query mapping

Queries are `GET /query?zql=<statement>`. ZigTSDB stores the metric name as a
`__metric__` pseudo-label, so a query over *N* CPU fields resolves them all in
**one statement** with a `__metric__ =~ 'cpu_(field_a|field_b|…)'` matcher and
folds per metric with `GROUP BY __metric__` — one round trip, one scan, the
long-form of QuestDB's wide `max(usage_user), max(usage_system), …` row.

The exception is `double-groupby-*`, which groups by hostname **and** metric;
multi-label `GROUP BY` is a follow-up, so those still emit *N* statements that
the runner executes sequentially and times together.

| TSBS query | ZQL |
|---|---|
| `single-groupby-1-h-t` | `SELECT max(value) FROM cpu_usage_user WHERE (hostname = 'host_a' OR …) AND time >= S AND time < E GROUP BY time_bucket(time, 1 m)` (one metric → plain windowed path) |
| `single-groupby-N-h-t` (N>1) | `SELECT max(value) FROM cpu_usage_user WHERE __metric__ =~ 'cpu_(usage_user|…)' AND (hostname = 'host_a' OR …) AND time … GROUP BY __metric__, time_bucket(time, 1 m)` — one statement |
| `cpu-max-all-N` | `SELECT max(value) FROM cpu_usage_user WHERE __metric__ =~ 'cpu_(usage_user|…|usage_guest_nice)' AND (hosts) AND time … GROUP BY __metric__, time_bucket(time, 1 h)` — one statement, all ten fields |
| `double-groupby-N` | `SELECT avg(value) FROM cpu_f WHERE time … GROUP BY hostname, time_bucket(time, 1 h)` — one statement per field `f` |
| `groupby-orderby-limit` | `SELECT max(value) FROM cpu_usage_user WHERE time < E GROUP BY time_bucket(time, 1 m) ORDER BY time DESC LIMIT 5` |
| `high-cpu-N` | `SELECT * FROM cpu_usage_user WHERE value > 90.0 [AND (hosts)] AND time … LIMIT 1000000` |
| `lastpoint` | `SELECT last(value) FROM cpu_usage_user GROUP BY hostname` |

Timestamps `S`/`E` are Unix nanoseconds. Notes:

- The `__metric__ =~` matcher is fully anchored, so `cpu_usage_guest` never
  matches `cpu_usage_guest_nice`. The response is one group per metric:
  `{"group_by":"__metric__","groups":[{"labels":{"__metric__":"cpu_usage_user"},"buckets":[…]}, …]}`.
- `high-cpu-*` returns only the `usage_user` column of qualifying rows;
  ZQL cannot assemble the other nine fields of the same reading.
- `lastpoint` needs ZigTSDB **1.2.0 or later** (the `last()` aggregate).
  Against an older server the runner stops with a query error.
- `GROUP BY hostname` is capped at 10,000 groups server-side, so
  `double-groupby-*` needs `--scale` ≤ 10000.
- Every statement stays under ZigTSDB's 2048-byte ZQL limit; the generator
  asserts it.

## Results (ZigTSDB 1.2.0, cpu-only devops)

Measured 2026-09-07 with this branch's target against the published image
`ghcr.io/samisoftb/zigtsdb@sha256:1ce7cf83f18920367a8a81055bd741c9c1cf8f2d046a2c79e42dac49944787f6` (tag `1.2.0`), QuestDB 9.x and VictoriaMetrics from
the same compose file, each engine limited to 2 CPU / 4 GB on a 2-CPU Docker VM (Intel
i9-9880H host), one engine stack at a time. Seed 123; loader 4 workers, batch 5000
readings; queries 2 workers, latency = per-query wall clock. Multi-metric queries use
this branch's one-statement form (`GROUP BY __metric__`). The correctness gate compares
ZigTSDB's answers with QuestDB's on the first 5 queries of 6 comparable types (tolerance
1e-6) and matched on both profiles.

Ingest rates are not like-for-like: the QuestDB loader writes ILP over TCP without
acknowledgement (a socket write rate), ZigTSDB waits for HTTP 200 per POST and
VictoriaMetrics for HTTP 204. ZigTSDB's standard-profile rate reflects per-series
ordering work on 40 000 interleaved series; sorted-by-series input loads several times
faster. `n/a`: VictoriaMetrics has no PromQL form for 4 query types; `high-cpu-*` on
ZigTSDB returns only the `usage_user` column; `(n×429)` marks statements ZigTSDB's
heavy-query valve deferred with `429`, retried after 1 s inside that row's latency.

### Profile `quick` — 100 hosts, 1 day at 60 s (1.44 M points), 50 queries per type

#### Ingest

| Engine | rows/s (readings) | metrics/s (points) |
|---|---:|---:|
| zigtsdb | 17115.31 | 171153.06 |
| questdb | 202541.37 | 2025413.69 |
| victoriametrics | 76764.36 | 767643.56 |

#### Query latency (ms) and throughput

| Query type | zigtsdb med / mean / max | zigtsdb q/s | questdb med / mean / max | questdb q/s | victoriametrics med / mean / max | victoriametrics q/s |
|---|---:|---:|---:|---:|---:|---:|
| cpu-max-all-1 | 3.25 / 3.69 / 9.49 | 538.71 | 34.68 / 45.58 / 278.35 | 43.57 | 3.29 / 3.97 / 13.86 | 498.38 |
| cpu-max-all-8 | 3.86 / 4.64 / 15.12 (1×429) | 429.53 | 29.04 / 31.59 / 73.87 | 62.66 | 7.51 / 8.13 / 19.11 | 237.99 |
| double-groupby-1 | 5.69 / 6.28 / 12.00 | 313.09 | 51.58 / 55.16 / 163.10 | 35.89 | 10.31 / 10.92 / 21.68 | 181.54 |
| double-groupby-5 | 34.63 / 45.30 / 137.74 | 43.78 | 44.60 / 47.23 / 94.63 | 42.18 | 44.59 / 44.30 / 74.83 | 44.55 |
| double-groupby-all | 49.19 / 57.54 / 92.42 | 34.72 | 44.22 / 48.57 / 92.71 | 41.06 | 70.44 / 74.30 / 118.77 | 26.69 |
| groupby-orderby-limit | 8.36 / 8.44 / 19.43 | 236.02 | 31.24 / 36.94 / 141.36 | 52.52 | n/a | |
| high-cpu-1 | 1.53 / 1.97 / 8.61 | 1005.48 | 7.88 / 12.14 / 80.59 | 162.87 | n/a | |
| high-cpu-all | 15.23 / 16.67 / 44.41 | 118.02 | 74.74 / 88.17 / 196.69 | 22.48 | n/a | |
| lastpoint | 21.20 / 21.07 / 32.59 | 94.79 | 10.04 / 11.12 / 32.08 | 178.01 | n/a | |
| single-groupby-1-1-1 | 2.61 / 3.28 / 12.17 | 606.05 | 12.79 / 13.68 / 31.35 | 145.28 | 3.20 / 3.60 / 8.85 | 544.22 |
| single-groupby-1-1-12 | 4.06 / 4.42 / 11.52 | 448.47 | 24.57 / 26.09 / 49.73 | 76.43 | 5.42 / 6.46 / 18.14 | 304.97 |
| single-groupby-1-8-1 | 1.73 / 2.16 / 9.41 | 917.02 | 11.07 / 12.18 / 24.00 | 160.78 | 3.57 / 3.97 / 10.78 | 495.45 |
| single-groupby-5-1-1 | 2.13 / 2.60 / 10.12 | 761.21 | 25.33 / 28.63 / 96.66 | 69.72 | 12.50 / 13.93 / 57.16 | 142.22 |
| single-groupby-5-1-12 | 7.96 / 8.24 / 14.19 | 239.24 | 24.01 / 34.29 / 199.07 | 57.42 | 30.84 / 37.69 / 123.73 | 52.66 |
| single-groupby-5-8-1 | 4.87 / 5.38 / 16.18 | 367.58 | 23.84 / 24.85 / 52.18 | 80.09 | 10.11 / 10.10 / 21.00 | 197.10 |

n/a: VictoriaMetrics has no PromQL form for 4 query types; ZigTSDB `lastpoint` needs the `last()` aggregate (≥ 1.2.0).
FAIL on ZigTSDB images ≤ 1.1.1 = HTTP 500 from their 2 MiB response-size cap (fixed in 1.2.0).
`high-cpu-*` on ZigTSDB returns only the `usage_user` column. `(n×429)`: statements the ZigTSDB heavy-query valve
deferred with `429 query_capacity`; the 1 s retry sleeps are included in that row's latency.
Ingest: the QuestDB loader writes ILP over TCP without acknowledgement (socket write rate); ZigTSDB waits for
HTTP 200 per POST, VictoriaMetrics for HTTP 204.

### Profile `standard` — 4000 hosts, 1 day at 60 s (57.6 M points), 200 queries per type

#### Ingest

| Engine | rows/s (readings) | metrics/s (points) |
|---|---:|---:|
| zigtsdb | 2652.18 | 26521.78 |
| questdb | 31719.87 | 317198.70 |
| victoriametrics | 75401.47 | 754014.71 |

#### Query latency (ms) and throughput

| Query type | zigtsdb med / mean / max | zigtsdb q/s | questdb med / mean / max | questdb q/s | victoriametrics med / mean / max | victoriametrics q/s |
|---|---:|---:|---:|---:|---:|---:|
| cpu-max-all-1 | 13.37 / 15.98 / 59.99 | 124.69 | 267.23 / 303.06 / 1416.45 | 6.58 | 12.85 / 16.11 / 104.36 | 123.84 |
| cpu-max-all-8 | 18.45 / 20.33 / 51.38 | 98.01 | 31.71 / 47.34 / 663.26 | 42.16 | 7.61 / 10.66 / 118.29 | 186.91 |
| double-groupby-1 | 144.80 / 154.25 / 257.92 (1×429) | 12.94 | 598.53 / 665.37 / 2509.05 | 3.00 | 250.64 / 272.20 / 665.89 | 7.33 |
| double-groupby-5 | 771.58 / 809.23 / 1377.98 | 2.46 | 919.65 / 956.18 / 1673.60 | 2.09 | 1113.98 / 1157.97 / 1912.58 | 1.73 |
| double-groupby-all | 1501.31 / 1537.00 / 1987.20 | 1.30 | 1964.16 / 2024.80 / 4126.46 (2×429) | 0.98 | 3537.15 / 3996.81 / 8514.05 | 0.50 |
| groupby-orderby-limit | 244.47 / 272.89 / 1315.26 | 7.32 | 318.37 / 346.82 / 1437.82 | 5.75 | n/a | |
| high-cpu-1 | 3.50 / 3.93 / 17.04 | 505.64 | 72.87 / 217.62 / 1877.18 | 8.96 | n/a | |
| high-cpu-all | 881.15 / 899.04 / 1630.46 | 2.22 | 2967.04 / 3109.98 / 5990.40 | 0.64 | n/a | |
| lastpoint | 418.00 / 456.48 / 1213.63 | 4.37 | 36.83 / 40.52 / 214.79 | 49.22 | n/a | |
| single-groupby-1-1-1 | 2.60 / 3.25 / 35.75 | 610.58 | 21.73 / 24.76 / 225.97 | 80.53 | 9.12 / 11.77 / 131.42 | 169.32 |
| single-groupby-1-1-12 | 4.75 / 5.78 / 40.47 | 344.63 | 22.80 / 28.27 / 355.73 | 70.42 | 8.40 / 10.21 / 44.13 | 194.67 |
| single-groupby-1-8-1 | 3.61 / 4.15 / 27.69 | 478.41 | 24.73 / 27.19 / 71.72 | 73.41 | 7.33 / 8.75 / 55.32 | 227.47 |
| single-groupby-5-1-1 | 20.80 / 23.23 / 96.16 | 85.72 | 49.21 / 60.72 / 268.24 | 32.84 | 18.39 / 23.24 / 183.94 | 85.76 |
| single-groupby-5-1-12 | 28.84 / 33.95 / 201.97 | 58.74 | 499.97 / 534.22 / 1750.40 | 3.72 | 33.94 / 36.26 / 100.83 | 54.86 |
| single-groupby-5-8-1 | 27.80 / 31.94 / 122.65 | 62.39 | 73.05 / 84.48 / 329.65 | 23.63 | 11.81 / 14.36 / 123.82 | 138.76 |

n/a: VictoriaMetrics has no PromQL form for 4 query types; ZigTSDB `lastpoint` needs the `last()` aggregate (≥ 1.2.0).
FAIL on ZigTSDB images ≤ 1.1.1 = HTTP 500 from their 2 MiB response-size cap (fixed in 1.2.0).
`high-cpu-*` on ZigTSDB returns only the `usage_user` column. `(n×429)`: statements the ZigTSDB heavy-query valve
deferred with `429 query_capacity`; the 1 s retry sleeps are included in that row's latency.
Ingest: the QuestDB loader writes ILP over TCP without acknowledgement (socket write rate); ZigTSDB waits for
HTTP 200 per POST, VictoriaMetrics for HTTP 204.

ZigTSDB is fastest of the three on 11 of 15 query types in the quick profile and 10 of
15 in the standard profile; VictoriaMetrics leads the 8-host `cpu-max-all` and
`single-groupby-*-8-1` shapes at scale, QuestDB leads `lastpoint`.

## How to run the test

```bash
# build
cd $GOPATH/src/github.com/timescale/tsbs && make

# data: cpu-only, 4000 hosts, one hour at 10 s
tsbs_generate_data --use-case="cpu-only" --seed=123 --scale=4000 \
  --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-01T01:00:00Z" \
  --log-interval="10s" --format="zigtsdb" | gzip > /tmp/bulk_data/zigtsdb-data.gz

# load
NUM_WORKERS=4 BATCH_SIZE=5000 ./scripts/load/load_zigtsdb.sh

# queries
tsbs_generate_queries --use-case="cpu-only" --seed=123 --scale=4000 \
  --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-01T01:00:01Z" \
  --queries=1000 --query-type="single-groupby-1-1-1" --format="zigtsdb" \
  | gzip > /tmp/bulk_queries/queries_zigtsdb-single-groupby-1-1-1.gz
NUM_WORKERS=4 ./scripts/run_queries/run_queries_zigtsdb.sh
```

`scripts/full_cycle_minitest/full_cycle_minitest_zigtsdb.sh` runs a tiny
generate → load → all-query-types cycle against a local server.
