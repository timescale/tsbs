# Time Series Benchmark Suite (TSBS)
This repo contains code for benchmarking several time series databases,
including TimescaleDB, MongoDB, InfluxDB, CrateDB and Cassandra.
This code is based on a fork of work initially made public by InfluxDB
at https://github.com/influxdata/influxdb-comparisons.

Current databases supported:

+ TimescaleDB [(supplemental docs)](docs/timescaledb.md)
+ MongoDB [(supplemental docs)](docs/mongo.md)
+ InfluxDB [(supplemental docs)](docs/influx.md)
+ Cassandra [(supplemental docs)](docs/cassandra.md)
+ ClickHouse [(supplemental docs)](docs/clickhouse.md)
+ CrateDB [(supplemental docs)](docs/cratedb.md)
+ SiriDB [(supplemental docs)](docs/siridb.md)

## Overview

The **Time Series Benchmark Suite (TSBS)** is a collection of Go
programs that are used to generate datasets and then benchmark read
and write performance of various databases. The intent is to make the
TSBS extensible so that a variety of use cases (e.g., devops, IoT, 
finance, etc.), query types, and databases can be included and benchmarked. To
this end we hope to help prospective database administrators find the
best database for their needs and their workloads. Further, if you
are the developer of a time series database and want to include your
database in the TSBS, feel free to open a pull request to add it!

## Current use cases

Currently, TSBS supports two use cases. First one, 'dev ops', comes in two forms. 
The full form is used to generate, insert, and measure data from 9 'systems'
that could be monitored in a real world dev ops scenario (e.g., CPU,
memory, disk, etc). Together, these 9 systems generate 100 metrics
per reading interval. The alternate form focuses solely on CPU
metrics for a simpler, more streamlined use case. This use case generates
10 CPU metrics per reading.

In addition to metric readings, 'tags' (including the location
of the host, its operating system, etc) are generated for each host
with readings in the dataset. Each unique set of tags identifies
one host in the dataset and the number of different hosts generated is
defined by the `scale` flag (see below).

The second use case is meant to simulate the data load in an IoT environment.
This use case is based on data streaming from a set of trucks tied to a 
fictional trucking company. This use case will simulate gathering diagnostic
data and metrics from each truck, and will also introduce environmental factors
such as out of order data and batch ingestion (for trucks 
that are offline for a period of time). We are also tracking truck metadata 
and using this to tie metrics and diagnostics together as part of the query set.  

The queries that are generated as part of this use case will cover both real 
time truck status and analytics that will look at the time series data in 
an effort to be more predictive about truck behavior.  The scale factor with 
this use case will be based on the number of trucks tracked.  

## What the TSBS tests

TSBS is used to benchmark bulk load performance and
query execution performance. (It currently does not measure
concurrent insert and query performance, which is a future priority.)
To accomplish this in a fair way, the data to be inserted and the
queries to run are pre-generated and native Go clients are used
wherever possible to connect to each database (e.g., `mgo` for MongoDB).

Although the data is randomly generated, TSBS data and queries are
entirely deterministic. By supplying the same PRNG (pseudo-random number
generator) seed to the generation programs, each database is loaded
with identical data and queried using identical queries.

## Installation

TSBS is a collection of Go programs (with some auxiliary bash and Python
scripts). The easiest way to get and install the Go programs is to use
`go get` and then `go install`:
```bash
# Fetch TSBS and its dependencies
$ go get github.com/timescale/tsbs
$ cd $GOPATH/src/github.com/timescale/tsbs/cmd
$ go get ./...

# Install desired binaries. At a minimum this includes tsbs_generate_data,
# tsbs_generate_queries, one tsbs_load_* binary, and one tsbs_run_queries_*
# binary:
$ cd $GOPATH/src/github.com/timescale/tsbs/cmd
$ cd tsbs_generate_data && go install
$ cd ../tsbs_generate_queries && go install
$ cd ../tsbs_load_timescaledb && go install
$ cd ../tsbs_run_queries_timescaledb && go install

# Optionally, install all binaries:
$ cd $GOPATH/src/github.com/timescale/tsbs/cmd
$ go install ./...
```

## How to use TSBS

Using TSBS for benchmarking involves 3 phases: data and query
generation, data loading/insertion, and query execution.

### Data and query generation

So that benchmarking results are not affected by generating data or
queries on-the-fly, with TSBS you generate the data and queries you want
to benchmark first, and then you can (re-)use it as input to the
benchmarking phases.

#### Data generation

Variables needed:
1. a use case. E.g., `cpu-only` (choose from `cpu-only` or `devops`)
1. a PRNG seed for deterministic generation. E.g., `123`
1. the number of devices to generate for. E.g., `4000`
1. a start time for the data's timestamps. E.g., `2016-01-01T00:00:00Z`
1. an end time. E.g., `2016-01-04T00:00:00Z`
1. how much time should be between each reading per device, in seconds. E.g., `10s`
1. and which database(s) you want to generate for. E.g., `timescaledb`
 (choose from `cassandra`, `clickhouse`, `cratedb`, `influx`, `mongo`, `siridb`,
  or `timescaledb`)

Given the above steps you can now generate a dataset (or multiple
datasets, if you chose to generate for multiple databases) that can
be used to benchmark data loading of the database(s) chosen using
the `tsbs_generate_data` tool:
```bash
$ tsbs_generate_data -use-case="cpu-only" -seed=123 -scale=4000 \
    -timestamp-start="2016-01-01T00:00:00Z" \
    -timestamp-end="2016-01-04T00:00:00Z" \
    -log-interval="10s" -format="timescaledb" \
    | gzip > /tmp/timescaledb-data.gz

# Each additional database would be a separate call.
```
_Note: We pipe the output to gzip to reduce on-disk space._

The example above will generate a pseudo-CSV file that can be used to
bulk load data into TimescaleDB. Each database has it's own format of how
it stores the data to make it easiest for its corresponding loader to
write data. The above configuration will generate just over 100M rows
(1B metrics), which is usually a good starting point.
Increasing the time period by a day will add an additional ~33M rows
so that, e.g., 30 days would yield a billion rows (10B metrics)

##### IoT use case

The main difference between the `iot` use case and other use cases is that 
it generates data which can contain out-of-order, missing, or empty 
entries to better represent real-life scenarios associated to the use case. 
Using a specified seed means that we can do this in a deterministic and 
reproducible way for multiple runs of data generation.

#### Query generation

Variables needed:
1. the same use case, seed, # of devices, and start time as used in data generation
1. an end time that is one second after the end time from data generation. E.g., for `2016-01-04T00:00:00Z` use `2016-01-04T00:00:01Z`
1. the number of queries to generate. E.g., `1000`
1. and the type of query you'd like to generate. E.g., `single-groupby-1-1-1` or `last-loc`

For the last step there are numerous queries to choose from, which are
listed in [Appendix I](#appendix-i-query-types). Additionally, the file
`scripts/generate_queries.sh` contains a list of all of them as the
default value for the environmental variable `QUERY_TYPES`. If you are
generating more than one type of query, we recommend you use that
helper script.

For generating just one set of queries for a given type:
```bash
$ tsbs_generate_queries -use-case="cpu-only" -seed=123 -scale=4000 \
    -timestamp-start="2016-01-01T00:00:00Z" \
    -timestamp-end="2016-01-04T00:00:01Z" \
    -queries=1000 -query-type="single-groupby-1-1-1" -format="timescaledb" \
    | gzip > /tmp/timescaledb-queries-single-groupby-1-1-1.gz
```

For generating sets of queries for multiple types:
```bash
$ FORMATS="timescaledb" SCALE=4000 SEED=123 \
    TS_START="2016-01-01T00:00:00Z" \
    TS_END="2016-01-04T00:00:01Z" \
    QUERIES=1000 QUERY_TYPES="single-groupby-1-1-1 single-groupby-1-1-12 double-groupby-1" \
    BULK_DATA_DIR="/tmp/bulk_queries" scripts/generate_queries.sh
```

A full list of query types can be found in
[Appendix I](#appendix-i-query-types) at the end of this README.

### Benchmarking insert/write performance

TSBS measures insert/write performance by taking the data generated in
the previous step and using it as input to a database-specific command
line program. To the extent that insert programs can be shared, we have
made an effort to do that (e.g., the TimescaleDB loader can
be used with a regular PostgreSQL database if desired). Each loader does
share some common flags -- e.g., batch size (number of readings inserted
together), workers (number of concurrently inserting clients), connection
details (host & ports), etc -- but they also have database-specific tuning
flags. To find the flags for a particular database, use the `-help` flag
(e.g., `tsbs_load_timescaledb -help`).

Instead of calling these binaries directly, we also supply
`scripts/load_<database>.sh` for convenience with many of the flags set
to a reasonable default for some of the databases.
So for loading into TimescaleDB, ensure that TimescaleDB is running and
then use:
```bash
# Will insert using 2 clients, batch sizes of 10k, from a file
# named `timescaledb-data.gz` in directory `/tmp`
$ NUM_WORKERS=2 BATCH_SIZE=10000 BULK_DATA_DIR=/tmp \
    scripts/load_timescaledb.sh
```

This will create a new database called `benchmark` where the data is
stored. It **will overwrite** the database if it exists; if you don't
want that to happen, supply a different `DATABASE_NAME` to the above
command.

---

By default, statistics about the load performance are printed every 10s,
and when the full dataset is loaded the looks like this:
```text
time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s
# ...
1518741528,914996.143291,9.652000E+08,1096817.886674,91499.614329,9.652000E+07,109681.788667
1518741548,1345006.018902,9.921000E+08,1102333.152918,134500.601890,9.921000E+07,110233.315292
1518741568,1149999.844750,1.015100E+09,1103369.385320,114999.984475,1.015100E+08,110336.938532

Summary:
loaded 1036800000 metrics in 936.525765sec with 8 workers (mean rate 1107070.449780/sec)
loaded 103680000 rows in 936.525765sec with 8 workers (mean rate 110707.044978/sec)
```

All but the last two lines contain the data in CSV format, with column names in the header. Those column names correspond to:
* timestamp,
* metrics per second in the period,
* total metrics inserted,
* overall metrics per second,
* rows per second in the period,
* total number of rows,
* overall rows per second.

For databases, like Cassandra, that do not use rows when inserting,
the last three values are always empty (indicated with a `-`).

The last two lines are a summary of how many metrics (and rows where
applicable) were inserted, the wall time it took, and the average rate
of insertion.

### Benchmarking query execution performance

To measure query execution performance in TSBS, you first need to load
the data using the previous section and generate the queries as
described earlier. Once the data is loaded and the queries are generated,
just use the corresponding `tsbs_run_queries_` binary for the database
being tested:
```bash
$ cat /tmp/queries/timescaledb-cpu-max-all-eight-hosts-queries.gz | \
    gunzip | tsbs_run_queries_timescaledb --workers=8 \
        --postgres="host=localhost user=postgres sslmode=disable"
```

You can change the value of the `--workers` flag to
control the level of parallel queries run at the same time. The
resulting output will look similar to this:
```text
run complete after 1000 queries with 8 workers:
TimescaleDB max cpu all fields, rand    8 hosts, rand 12hr by 1h:
min:    51.97ms, med:   757.55, mean:  2527.98ms, max: 28188.20ms, stddev:  2843.35ms, sum: 5056.0sec, count: 2000
all queries                                                     :
min:    51.97ms, med:   757.55, mean:  2527.98ms, max: 28188.20ms, stddev:  2843.35ms, sum: 5056.0sec, count: 2000
wall clock time: 633.936415sec
```

The output gives you the description of the query and multiple groupings
of measurements (which may vary depending on the database).

---

For easier testing of multiple queries, we provide
`scripts/generate_run_script.py` which creates a bash script with commands
to run multiple query types in a row. The queries it generates should be
put in a file with one query per line and the path given to the script.
For example, if you had a file named `queries.txt` that looked like this:
```text
high-cpu-1
cpu-max-all-8
groupby-orderby-limit
double-groupby-1
```

You could generate a run script named `query_test.sh`:
```bash
# Generate run script for TimescaleDB, using queries in `queries.txt`
# with the generated query files in /tmp/queries for 8 workers
$ python generate_run_script.py -d timescaledb -o /tmp/queries \
    -w 8 -f queries.txt > query_test.sh
```

And the resulting script file would look like:
```bash
#!/bin/bash
# Queries
cat /tmp/queries/timescaledb-high-cpu-1-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-high-cpu-1-queries.out

cat /tmp/queries/timescaledb-cpu-max-all-8-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-cpu-max-all-8-queries.out

cat /tmp/queries/timescaledb-groupby-orderby-limit-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-groupby-orderby-limit-queries.out

cat /tmp/queries/timescaledb-double-groupby-1-queries.gz | gunzip | query_benchmarker_timescaledb --workers=8 --limit=1000 --hosts="localhost" --postgres="user=postgres sslmode=disable"  | tee query_timescaledb_timescaledb-double-groupby-1-queries.out
```

### Query validation (optional)

Additionally each `tsbs_run_queries_` binary allows you print the
actual query results so that you can compare across databases that the
results are the same. Using the flag `-print-responses` will return
the results.

## Appendix I: Query types <a name="appendix-i-query-types"></a>

### Devops / cpu-only
|Query type|Description|
|:---|:---|
|single-groupby-1-1-1| Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1 hour
|single-groupby-1-1-12| Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12 hours
|single-groupby-1-8-1| Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1 hour
|single-groupby-5-1-1| Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour
|single-groupby-5-1-12| Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hours
|single-groupby-5-8-1| Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour
|cpu-max-all-1| Aggregate across all CPU metrics per hour over 1 hour for a single host
|cpu-max-all-8| Aggregate across all CPU metrics per hour over 1 hour for eight hosts
|double-groupby-1| Aggregate on across both time and host, giving the average of 1 CPU metric per host per hour for 24 hours
|double-groupby-5| Aggregate on across both time and host, giving the average of 5 CPU metrics per host per hour for 24 hours
|double-groupby-all| Aggregate on across both time and host, giving the average of all (10) CPU metrics per host per hour for 24 hours
|high-cpu-all| All the readings where one metric is above a threshold across all hosts
|high-cpu-1| All the readings where one metric is above a threshold for a particular host
|lastpoint| The last reading for each host
|groupby-orderby-limit| The last 5 aggregate readings (across time) before a randomly chosen endpoint

### IoT
|Query type|Description|
|:---|:---|
|last-loc|Fetch real-time (i.e. last) location of each truck
|low-fuel|Fetch all trucks with low fuel (less than 10%)
|high-load|Fetch trucks with high current load (over 90% load capacity)
|stationary-trucks|Fetch all trucks that are stationary (low avg velocity in last 10 mins)
|long-driving-sessions|Get trucks which haven't rested for at least 20 mins in the last 4 hours
|long-daily-sessions|Get trucks which drove more than 10 hours in the last 24 hours
|avg-vs-projected-fuel-consumption|Calculate average vs. projected fuel consumption per fleet
|avg-daily-driving-duration|Calculate average daily driving duration per driver
|avg-daily-driving-session|Calculate average daily driving session per driver
|avg-load|Calculate average load per truck model per fleet
|daily-activity|Get the number of hours truck has been active (vs. out-of-commission) per day per fleet
|breakdown-frequency|Calculate breakdown frequency by truck model
