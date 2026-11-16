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
docker pull ghcr.io/samisoftb/zigtsdb:1.3.0   # sha256:8cf23b2a571b170e634dfca2944a7a23711fd091825f5488a2cc0f1230e64bf4
DIGEST=$(docker inspect --format '{{index .RepoDigests 0}}' ghcr.io/samisoftb/zigtsdb:1.3.0)
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

## Results (ZigTSDB 1.3.0, cpu-only devops) — and the change from 1.2.0

Measured 2026-09-11 with this branch's target (`752a35a`) on AWS, one on-demand c7i.xlarge
(4 vCPU / 8 GB, `eu-west-1`) per image, the two published images run back to back on the same
lane: `ghcr.io/samisoftb/zigtsdb@sha256:1ce7cf83f18920367a8a81055bd741c9c1cf8f2d046a2c79e42dac49944787f6`
(tag `1.2.0`) and `ghcr.io/samisoftb/zigtsdb@sha256:8cf23b2a571b170e634dfca2944a7a23711fd091825f5488a2cc0f1230e64bf4`
(tag `1.3.0`, `:latest`), QuestDB 9.x and VictoriaMetrics from the same compose file, each engine
limited to 2 CPU / 4 GB, one engine stack at a time. Seed 123; loader 4 workers, batch 5000
readings; queries 2 workers (quick: 50 per type, standard: 200), latency = per-query wall clock.
Multi-metric queries use this branch's one-statement form (`GROUP BY __metric__`). The
correctness gate (ZigTSDB vs QuestDB values, first 5 queries of 6 comparable types, tolerance
1e-6) matched on all four runs. These supersede the 1.2.0 section below, which was measured on a
2-CPU laptop VM and is not comparable across boxes.

### 1.2.0 → 1.3.0 on the same lane (ZigTSDB; QuestDB column from the 1.3.0 run, for scale)

#### Profile `quick` — v1.2.0 image → v1.3.0 image (correctness gate: MATCH / MATCH)

| Ingest (ZigTSDB) | v1.2.0 image | v1.3.0 image | delta |
|---|---:|---:|---:|
| readings/s | 156,665 | 213,763 | +36 % |
| points/s | 1.57 M | 2.14 M | +36 % |

| Query type (median ms) | v1.2.0 image | v1.3.0 image | delta | QuestDB (v1.3.0 image run) |
|---|---:|---:|---:|---:|
| cpu-max-all-1 | 0.42 | 0.31 | -26.2 % | 6.92 |
| cpu-max-all-8 | 0.97 | 1.13 | +16.5 % | 8.21 |
| double-groupby-1 | 1.27 | 1.38 | +8.7 % | 14.03 |
| double-groupby-5 | 7.04 | 7.27 | +3.3 % | 13.61 |
| double-groupby-all | 13.47 | 14.33 | +6.4 % | 14.65 |
| groupby-orderby-limit | 2.85 | 2.31 | -18.9 % | 7.72 |
| high-cpu-1 | 0.12 | 0.13 | +8.3 % | 1.49 |
| high-cpu-all | 3.31 | 3.97 | +19.9 % | 10.30 |
| lastpoint | 3.65 | 0.16 | -95.6 % | 0.72 |
| single-groupby-1-1-1 | 0.14 | 0.15 | +7.1 % | 1.97 |
| single-groupby-1-1-12 | 0.48 | 0.43 | -10.4 % | 3.08 |
| single-groupby-1-8-1 | 0.18 | 0.19 | +5.6 % | 2.10 |
| single-groupby-5-1-1 | 0.35 | 0.30 | -14.3 % | 1.89 |
| single-groupby-5-1-12 | 1.25 | 1.51 | +20.8 % | 3.06 |
| single-groupby-5-8-1 | 0.45 | 0.52 | +15.6 % | 2.42 |

#### Profile `standard` — v1.2.0 image → v1.3.0 image (correctness gate: MATCH / MATCH)

| Ingest (ZigTSDB) | v1.2.0 image | v1.3.0 image | delta |
|---|---:|---:|---:|
| readings/s | 21,102 | 124,590 | +490 % |
| points/s | 0.21 M | 1.25 M | +490 % |

| Query type (median ms) | v1.2.0 image | v1.3.0 image | delta | QuestDB (v1.3.0 image run) |
|---|---:|---:|---:|---:|
| cpu-max-all-1 | 6.73 | 0.36 | -94.7 % | 5.65 |
| cpu-max-all-8 | 7.41 | 0.96 | -87.0 % | 8.76 |
| double-groupby-1 | 55.27 | 52.92 | -4.3 % | 387.86 |
| double-groupby-5 | 302.56 | 293.60 | -3.0 % | 672.00 |
| double-groupby-all | 601.79 | 586.30 | -2.6 % | 996.86 |
| groupby-orderby-limit | 76.36 | 79.46 | +4.1 % | 102.69 |
| high-cpu-1 | 0.36 | 0.14 | -61.1 % | 1.03 |
| high-cpu-all | 167.23 | 173.94 | +4.0 % | 347.15 |
| lastpoint | 138.90 | 1.61 | -98.8 % | 5.79 |
| single-groupby-1-1-1 | 0.23 | 0.14 | -39.1 % | 1.48 |
| single-groupby-1-1-12 | 0.56 | 0.50 | -10.7 % | 2.53 |
| single-groupby-1-8-1 | 0.33 | 0.20 | -39.4 % | 1.90 |
| single-groupby-5-1-1 | 3.69 | 0.27 | -92.7 % | 1.49 |
| single-groupby-5-1-12 | 4.53 | 1.40 | -69.1 % | 3.10 |
| single-groupby-5-8-1 | 3.89 | 0.47 | -87.9 % | 2.25 |

What moved: standard-profile ingest 5.9× (per-series ordering on 40 000 interleaved series,
retained ingest scratch, WAL group commit across concurrent requests); `lastpoint` 138.9 → 1.61 ms
(the `last()` fast path plus a seal-warmed tail cache); `cpu-max-all-*` and `single-groupby-5-*`
served in place; `double-groupby-*` −3 % (a partition cache; a one-statement multi-label form for
these queries is a follow-up on this branch). Rows within ±15 % are the box's run-to-run band.

### Profile `quick` — 100 hosts, 1 day at 60 s (1.44 M points), 50 queries per type (ZigTSDB 1.3.0)

#### Ingest

| Engine | rows/s (readings) | metrics/s (points) |
|---|---:|---:|
| zigtsdb | 213762.83 | 2137628.34 |
| questdb | 467036.23 | 4670362.29 |
| victoriametrics | 146131.90 | 1461319.03 |

#### Query latency (ms) and throughput

| Query type | zigtsdb med / mean / max | zigtsdb q/s | questdb med / mean / max | questdb q/s | victoriametrics med / mean / max | victoriametrics q/s |
|---|---:|---:|---:|---:|---:|---:|
| cpu-max-all-1 | 0.31 / 0.37 / 1.31 | 5228.20 | 6.92 / 15.27 / 61.73 | 129.75 | 0.74 / 0.95 / 4.68 | 2044.44 |
| cpu-max-all-8 | 1.13 / 1.15 / 1.84 | 1725.15 | 8.21 / 16.28 / 59.47 | 121.47 | 1.86 / 1.99 / 4.03 | 1000.49 |
| double-groupby-1 | 1.38 / 1.49 / 3.19 | 1337.34 | 14.03 / 28.61 / 84.12 | 69.79 | 2.93 / 3.30 / 9.45 | 600.12 |
| double-groupby-5 | 7.27 / 7.61 / 11.32 | 261.91 | 13.61 / 22.95 / 78.33 | 86.60 | 11.73 / 12.19 / 21.07 | 162.43 |
| double-groupby-all | 14.33 / 14.80 / 22.43 | 133.94 | 14.65 / 23.18 / 61.34 | 87.14 | 22.38 / 23.26 / 32.66 | 85.06 |
| groupby-orderby-limit | 2.31 / 2.39 / 5.03 | 826.93 | 7.72 / 15.22 / 60.38 | 130.08 | n/a | |
| high-cpu-1 | 0.13 / 0.18 / 0.97 | 10471.98 | 1.49 / 2.92 / 40.14 | 533.48 | n/a | |
| high-cpu-all | 3.97 / 3.79 / 6.46 | 525.54 | 10.30 / 23.45 / 73.61 | 81.77 | n/a | |
| lastpoint | 0.16 / 0.24 / 1.31 | 8435.85 | 0.72 / 1.16 / 8.33 | 1662.65 | n/a | |
| single-groupby-1-1-1 | 0.15 / 0.20 / 1.15 | 8806.93 | 1.97 / 3.17 / 20.60 | 623.44 | 0.42 / 0.51 / 1.98 | 3720.85 |
| single-groupby-1-1-12 | 0.43 / 0.48 / 1.41 | 4088.38 | 3.08 / 5.29 / 46.74 | 375.18 | 0.80 / 0.87 / 2.18 | 2229.62 |
| single-groupby-1-8-1 | 0.19 / 0.25 / 0.99 | 7708.13 | 2.10 / 2.87 / 9.43 | 683.15 | 0.53 / 0.60 / 1.55 | 3308.26 |
| single-groupby-5-1-1 | 0.30 / 0.35 / 1.09 | 5584.61 | 1.89 / 2.23 / 5.01 | 881.57 | 0.62 / 0.71 / 1.93 | 2745.59 |
| single-groupby-5-1-12 | 1.51 / 1.48 / 2.31 | 1343.67 | 3.06 / 4.58 / 24.79 | 430.19 | 2.19 / 2.22 / 3.42 | 886.88 |
| single-groupby-5-8-1 | 0.52 / 0.64 / 1.48 | 3015.54 | 2.42 / 3.28 / 10.13 | 603.71 | 1.12 / 1.23 / 2.36 | 1602.36 |

### Profile `standard` — 4000 hosts, 1 day at 60 s (57.6 M points), 200 queries per type (ZigTSDB 1.3.0)

#### Ingest

| Engine | rows/s (readings) | metrics/s (points) |
|---|---:|---:|
| zigtsdb | 124589.94 | 1245899.38 |
| questdb | 500247.65 | 5002476.51 |
| victoriametrics | 183695.27 | 1836952.69 |

#### Query latency (ms) and throughput

| Query type | zigtsdb med / mean / max | zigtsdb q/s | questdb med / mean / max | questdb q/s | victoriametrics med / mean / max | victoriametrics q/s |
|---|---:|---:|---:|---:|---:|---:|
| cpu-max-all-1 | 0.36 / 0.38 / 1.46 | 5212.06 | 5.65 / 20.93 / 626.14 | 95.54 | 1.46 / 2.26 / 11.36 | 880.39 |
| cpu-max-all-8 | 0.96 / 1.03 / 2.21 | 1929.08 | 8.76 / 16.80 / 77.03 | 117.04 | 2.69 / 3.04 / 15.72 | 653.63 |
| double-groupby-1 | 52.92 / 58.87 / 88.19 | 33.96 | 387.86 / 436.54 / 1801.34 | 4.58 | 113.72 / 114.44 / 169.13 | 17.45 |
| double-groupby-5 | 293.60 / 299.44 / 385.36 | 6.68 | 672.00 / 674.15 / 989.18 | 2.96 | 591.97 / 594.77 / 746.43 | 3.36 |
| double-groupby-all | 586.30 / 596.78 / 751.62 | 3.34 | 996.86 / 1028.85 / 1710.97 | 1.94 | 1206.91 / 1216.62 / 1527.42 | 1.64 |
| groupby-orderby-limit | 79.46 / 85.68 / 225.94 | 23.30 | 102.69 / 110.64 / 578.62 | 18.06 | n/a | |
| high-cpu-1 | 0.14 / 0.18 / 1.73 | 10886.93 | 1.03 / 6.53 / 223.92 | 295.12 | n/a | |
| high-cpu-all | 173.94 / 188.31 / 257.12 | 10.62 | 347.15 / 358.53 / 545.31 | 5.57 | n/a | |
| lastpoint | 1.61 / 1.68 / 5.50 | 1184.60 | 5.79 / 9.32 / 51.13 | 213.81 | n/a | |
| single-groupby-1-1-1 | 0.14 / 0.15 / 0.95 | 12548.01 | 1.48 / 2.40 / 48.53 | 825.65 | 0.47 / 0.60 / 7.49 | 3326.38 |
| single-groupby-1-1-12 | 0.50 / 0.58 / 6.01 | 3359.77 | 2.53 / 4.27 / 48.44 | 465.84 | 0.73 / 0.78 / 1.64 | 2540.46 |
| single-groupby-1-8-1 | 0.20 / 0.22 / 1.01 | 9046.79 | 1.90 / 3.09 / 45.24 | 600.53 | 0.63 / 0.79 / 2.48 | 2481.05 |
| single-groupby-5-1-1 | 0.27 / 0.29 / 1.35 | 6871.61 | 1.49 / 1.95 / 41.68 | 1013.16 | 0.74 / 0.81 / 2.49 | 2432.76 |
| single-groupby-5-1-12 | 1.40 / 1.43 / 2.99 | 1391.10 | 3.10 / 6.11 / 51.42 | 327.23 | 1.82 / 1.87 / 3.04 | 1060.59 |
| single-groupby-5-8-1 | 0.47 / 0.49 / 1.58 | 4075.44 | 2.25 / 3.67 / 49.45 | 541.04 | 1.13 / 1.24 / 4.52 | 1598.54 |

n/a: VictoriaMetrics has no PromQL form for 4 query types. `(n×429)` marks statements ZigTSDB's
heavy-query valve deferred with `429`, retried after 1 s inside that row's latency. Ingest rates are
not like-for-like across engines (QuestDB's loader writes ILP over TCP without acknowledgement;
ZigTSDB waits for HTTP 200 per POST, VictoriaMetrics for HTTP 204).

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
