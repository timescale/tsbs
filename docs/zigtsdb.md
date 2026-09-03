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
docker pull ghcr.io/samisoftb/zigtsdb:1.1.1
DIGEST=$(docker inspect --format '{{index .RepoDigests 0}}' ghcr.io/samisoftb/zigtsdb:1.1.1)
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

Queries are `GET /query?zql=<statement>`. ZQL has no cross-metric `SELECT`,
so a TSBS query over *N* CPU fields is generated as *N* statements. The
runner executes them **sequentially on one keep-alive connection and reports
the wall-clock time over all of them** as the query latency. Keep that in
mind when comparing with engines that answer the same query in one request.

| TSBS query | ZQL (one statement per CPU field `f`) |
|---|---|
| `single-groupby-N-h-t` | `SELECT max(value) FROM cpu_f WHERE (hostname = 'host_a' OR …) AND time >= S AND time < E GROUP BY time_bucket(time, 1 m)` |
| `cpu-max-all-N` | `SELECT max(value) FROM cpu_f WHERE (hosts) AND time … GROUP BY time_bucket(time, 1 h)` — 10 statements |
| `double-groupby-N` | `SELECT avg(value) FROM cpu_f WHERE time … GROUP BY hostname, time_bucket(time, 1 h)` |
| `groupby-orderby-limit` | `SELECT max(value) FROM cpu_usage_user WHERE time < E GROUP BY time_bucket(time, 1 m) ORDER BY time DESC LIMIT 5` |
| `high-cpu-N` | `SELECT * FROM cpu_usage_user WHERE value > 90.0 [AND (hosts)] AND time … LIMIT 1000000` |
| `lastpoint` | `SELECT last(value) FROM cpu_usage_user GROUP BY hostname` |

Timestamps `S`/`E` are Unix nanoseconds. Notes:

- `high-cpu-*` returns only the `usage_user` column of qualifying rows;
  ZQL cannot assemble the other nine fields of the same reading.
- `lastpoint` needs ZigTSDB **1.2.0 or later** (the `last()` aggregate).
  Against an older server the runner stops with a query error.
- `GROUP BY hostname` is capped at 10,000 groups server-side, so
  `double-groupby-*` needs `--scale` ≤ 10000.
- Every statement stays under ZigTSDB's 2048-byte ZQL limit; the generator
  asserts it.

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
