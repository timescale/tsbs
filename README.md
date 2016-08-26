# influxdb-comparisons
This repo contains code for benchmarking InfluxDB against other databases and time series solutions. You can access the [detailed technical writeups for each here](https://influxdata.com/technical-papers/).

Current databases supported:

+ InfluxDB
+ Elasticsearch ([announcement blog here](https://influxdata.com/blog/influxdb-markedly-elasticsearch-in-time-series-data-metrics-benchmark/))

## Testing Methodology
In an attempt to make our performance comparison both realistic and relatable, we decided to build our benchmark suite according to real-world use cases. Micro-benchmarks are useful for database engineers, but using realistic data helps us better understand how our software performs under practical workloads.

Currently, the benchmarking tools focus on the DevOps use case. We create data and queries that mimic what a system administrator would see when operating a fleet of hundreds or thousands of virtual machines. We create and query values like CPU load; RAM usage; number of active, sleeping, or stalled processes; and disk used. Future benchmarks will expand to include the IoT and application monitoring use cases.

We benchmark bulk load performance and synchronous query execution performance. The benchmark suite is written in Go, and attempts to be as fair to each database as possible by removing test-related computational overhead (by pre-generating our datasets and queries). 

Although the data is randomly generated, our data and queries are entirely deterministic. By supplying the same PRNG (pseudo-random number generator) seed to the test generation code, each database is loaded with identical data and queried using identical queries.

(Note: The use of more than one worker thread does lead to a non-deterministic ordering of events when writing and/or querying the databases.)

There are five phases when using the benchmark suite: data generation, data loading, query generation, query execution, and query validation.

### Phase 1: Data generation

Each benchmark begins with data generation. 

The DevOps data generator creates time series points that correspond to server telemetry, similar to what a server fleet would send at regular intervals to a metrics collections service (like Telegraf or collectd). Our DevOps data generator runs a simulation for a pre-specified number of hosts, and emits serialized points to stdout. For each simulated machine, nine different measurements are written in 10-second intervals.

The intended usage of the DevOps data generator is to create distinct datasets that simulate larger and larger server fleets over increasing amounts of time. As the host count or the time interval go up, the point count increases. This approach lets us examine how the databases scale on a real-world workload in the dimensions our DevOps users care about.

Each simulated host is initialized with a RAM size and a set of stateful probability distributions (Gaussian random walks with clamping), corresponding to nine statistics as reported by Telegraf. Here are the Telegraf collectors for CPU and memory:

https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/cpu.go
https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/memory.go

For example, here is a graph of the simulated CPU usage through time for 10 hosts, when using the data generator:

(TODO screenshot of graph from Chronograf)

And, here is a graph of the simulated memory from the same simulation:

(TODO screenshot of graph from Chronograf)

Note that the generator shares its simulation logic between databases. This is not just for code quality; we did this to ensure that the generated data is, within floating point tolerances, exactly the same for each database.

A DevOps dataset is fully specified by the following parameters:
Number of hosts to simulate (default 1)
Start time (default January 1st 2016 at midnight, inclusive)
End time (default January 2nd 2016 at midnight, exclusive)
PRNG seed (default uses the current time)

The ‘scaling variable’ for the DevOps generator is the number of hosts to simulate. By default, the data is generated over a simulated period of one day. Each simulated host produces nine measurements per 10-second epoch, one each of:

+ cpu
+ diskio
+ disk
+ kernel
+ mem
+ net
+ nginx
+ postgresl
+ redis

Each measurement holds different values that are being stored. In total, all nine measurements store 100 field values.

The following equations describe how many points are generated for a 24 hour period:

```
seconds_in_day = (24 hours in a day) * (60 minutes in an hour) * (60 seconds in a minute) = 86,400 seconds
epochs = seconds_in_day / 10 = 8,640
point_count = epochs * host_count * 9
```

So, for one host we get 8,640 * 1 * 9 = 77,760 points, and for 1,000 hosts we get 8,640 * 1000 * 9 = 77,760,000 points.

For these benchmarks, we generated a dataset we call DevOps-100: 100 simulated hosts over various time periods (1-4 days).

Generated data is written in a database-specific format that directly equates to the bulk write protocol of each database. This helps make the following benchmark, bulk loading, as straightforward as possible.

For InfluxDB, the bulk load protocol is described at:
https://docs.influxdata.com/influxdb/v0.12/guides/writing_data/#writing-multiple-points

For Elastic, the bulk load protocol is described at:
https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

Note that both of these formats are human-readable.

### Phase 2: Data loading

After data generation comes data loading.

The data loading programs stream data from stdin; typically, this is from a file created by the data generator. As data is read, the loader performs a minimum of deserialization and queues up writes into a batch. As batches become ready, the points are loaded into the destination database as fast as possible.

(Each database currently has its own bulk loader program. In the future, we want to merge the programs together to minimize the amount of special-case code.)

#### Configuration

Each bulk loader takes a handful of parameters that affect performance:

ElasticSearch:
Number of workers to use to make bulk load writes in parallel,
Which index template to use (more on this later),
Whether to force an index refresh after each write, and
How many items to include in each write batch.

InfluxDB:
Number of workers to use to make bulk load writes in parallel, and
How many points to include in each write batch.

(For calibration, there is also an option to disable writing to the database; this mode is used to check the speed of data deserialization.)

Note that the bulk loaders will not start writing data if there is already data in the destination database at the beginning of a test. This helps ensure that the database is empty, as if it were newly-installed. It also prevents users from clobbering existing data.

#### Elasticsearch-specific configuration
Both Elasticsearch and InfluxDB are ready out-of-the-tarball for storing time series data. However, after meeting with Elasticsearch experts, we decided to make some reasonable configuration tweaks to Elasticsearch to try to optimize its performance.

First, the configuration for the Elasticsearch daemon was changed to set the ES_HEAP_SIZE environment variable to half of the server machine’s available RAM. For example, on a 32GB machine, ES_HEAP_SIZE is 16g. This is standard practice when administering Elasticsearch.

Second, the configuration file was also changed to increase the threadpool.bulk.queue_size parameter to 100000. When we tried bulk loading without this tweak, the server replied with errors indicating it had run out of buffer space for receiving bulk writes. This config change is standard practice for bulk write workloads.

Third, we developed two Elasticsearch index templates, each of which represents a way we think people use Elasticsearch to store time-series data:

The first template, called ‘default’, stores time-series data in a way that enables fast querying, while also storing the original document data. This is closest to Elasticsearch’s default behavior and is a reasonable starting point for most users, although its on-disk size may become large.

The second template, called ‘aggregation’, indexes time-series data in a way that saves disk space by discarding the original point data. All data is stored in a compressed form inside the Lucene indexes, therefore all queries are completely accurate. But, due to an implementation detail of Elastic, the underlying point data is no longer independently addressable. For users who only conduct aggregation queries, this saves quite a bit of disk space (and improves bulk load speed) without any downsides.

Fourth, after each bulk load in Elasticsearch, we trigger a forced compaction of all index data. This is not included in the speed measurements; we give this to Elasticsearch ‘for free’. We’ve chosen to do this because compactions occur continuously over the lifetime of a long-running Elasticsearch process, so this helps us obtain numbers that are representative of steady-state operation of Elasticsearch in production environments.

(Note that Elasticsearch does not immediately index data written with the bulk endpoint. To make written data immediately available for querying, users can set the URL query parameter ‘refresh’ to ‘true’. We didn’t do this because performance dropped considerably, and most users would not need this when performing a bulk load. InfluxDB performs an `fsync` after each bulk write, and makes data immediately available for querying.)

#### InfluxDB-specific configuration

The only change we made to a default InfluxDB install is to, like Elastic, cause a full database compaction after a bulk load benchmark is complete. This forces all eventual compaction to happen at once, simulating steady-state operation of the data store.

#### Measurements
For bulk loading, we care about two numerical outcomes: the total wall clock time taken to write the given dataset, and how much disk space is used by the database after all writes are complete.

When finished, the bulk load program prints out how long it took to load data, and what the  average ingestion rate was.

Combining the following parameters gives a hypothetical ‘performance matrix’ for a given dataset:

```
Client parallelism: 1, 2, 4, 8, 16
Database: InfluxDB, Elasticsearch (with default template), Elasticsearch (with aggregation template)
```

Which gives a possible set of 15 bulk write benchmarks. Running all these tests is excessive, but it is possible and allows us to confidently determine how both write throughput and disk usage scale. 


### Phase 3: Query generation

The third phase makes serialized queries and saves them to a file.

We pre-generate all queries before benchmarking them, so that the query benchmarker can be as lightweight as possible. This allows us to reuse code between the database drivers. It also lets us prove that the runtime overhead of query generation does not impact the benchmarks.

Many benchmark suites generate and serialize queries at the same time as running benchmarks; this is typically a mistake. For example, Elasticsearch takes queries in JSON format, yet InfluxDB has a simpler wire format. If we included query generation in the query  benchmarker, then the JSON serialization overhead would negatively, and unfairly, affect the Elasticsearch benchmark. 

(In the case of JSON this effect is especially acute: the JSON encoder in Go’s standard library makes many heap allocations and uses reflection.)

The DevOps use case is focused on relating to the the needs of system administrators. As we saw above, the data for our benchmark is telemetry from a simulated server fleet.

The queries that administrators tend to run are focused on: 1) visualizing information on dashboards, 2) identifying trends in system utilization, and 3) drilling down into a particular server’s behavior.

To that end, we have identified the following query types as being representative of a sysadmin’s needs:

```
Maximum CPU usage for 1 host, over the course of an hour, in 1 minute intervals
Maximum CPU usage for 2 hosts, over the course of an hour, in 1 minute intervals
Maximum CPU usage for 4 hosts, over the course of an hour, in 1 minute intervals
Maximum CPU usage for 8 hosts, over the course of an hour, in 1 minute intervals
Maximum CPU usage for 16 hosts, over the course of an hour, in 1 minute intervals
Maximum CPU usage for 32 hosts, over the course of an hour, in 1 minute intervals
```

Each of these six abstract query types are parameterized to create millions of concrete queries, which are then serialized to files. (For example, the max CPU query for one host will be parameterized on 1) a random host id, and 2) a random 60-minute interval.) These requests will be read by the query benchmarker and then sent to the database.

Our query generator program uses a deterministic random number generator to fill in the parameters for each concrete query. 

For example, here are two queries for InfluxDB that aggregate maximum CPU information for 2 hosts during a random 1-hour period, in 1 minute buckets. Each hostname was chosen from a set of 100 hosts, because in this example the Scaling Variable is `100`:

```
SELECT max(usage_user) FROM cpu WHERE (hostname = 'host_73' OR hostname = 'host_24') AND time >= '2016-01-01T19:24:45Z' AND time < '2016-01-01T20:24:45Z' GROUP BY time(1m)
SELECT max(usage_user) FROM cpu WHERE (hostname = 'host_60' OR hostname = 'host_79') AND time >= '2016-01-01T11:14:49Z' AND time < '2016-01-01T12:14:49Z' GROUP BY time(1m)
```

Notice that the time range is always 60 minutes long, and that the start of the time range is randomly chosen.


The result of the query generation step is two files of serialized queries, one for each database.

### Phase 4: Query execution

The final step is benchmarking query performance.

So far we have covered data generation, data loading, and query generation. Now, all of that culminates in a benchmark for each database that measures how fast they can satisfy queries.

Our query benchmarker is a small program that executes HTTP requests in parallel. It reads pre-generated requests from stdin, performs a minimum of deserialization, then executes those queries against the chosen endpoint. It supports making requests in parallel, and collects basic summary statistics during its execution.

The query benchmarker has zero knowledge of the database it is testing; it just executes HTTP requests and measures the outcome.

We use the [fasthttp](https://github.com/valyala/fasthttp "fasthttp") library for the HTTP client, because it minimizes heap allocations and can be up to 10x faster than Go’s default client.

Before every execution of the query benchmarker, we restart the given database daemon in order to flush any query caches.

### Phase 5: Query validation

The final step is to validate the benchmark by sampling the query results for both databases.

The benchmark suite was engineered to be fully deterministic. However, that does not guard against possible semantic mistakes in the data or query set. For example, queries for one database could be valid, yet wrong, if they compute an undesired result.

To show the parity of both data and queries between the databases, we can compare the query responses themselves.

Our query benchmarker tool has a mode for pretty-printing the query responses it receives. By running it in this mode, we can inspect query results and compare the results for each database.

For example, here is a side-by-side comparison of the responses for the same query (a list of maximums, in 1-minute buckets):

InfluxDB query response:
```
{
  "results": [
    {
      "series": [
        {
          "name": "cpu",
          "columns": [
            "time",
            "max"
          ],
          "values": [
            [
              "2016-01-01T18:29:00Z",
              90.92765387779365
            ],
            [
              "2016-01-01T18:30:00Z",
              89.58087379178397
            ],
            [
              "2016-01-01T18:31:00Z",
              88.39341429374308
            ],
            [
              "2016-01-01T18:32:00Z",
              84.27665178871197
            ],
            [
              "2016-01-01T18:33:00Z",
              84.95048030509422
            ],
            ...
```

Elasticsearch query response:
```
{
  "took": 133,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "failed": 0
  },
  "hits": {
    "total": 1728000,
    "max_score": 0.0,
    "hits": []
  },
  "aggregations": {
    "result": {
      "doc_count": 360,
      "result2": {
        "buckets": [
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451672940000,
            "doc_count": 4,
            "max_of_field": {
              "value": 90.92765387779365
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673000000,
            "doc_count": 6,
            "max_of_field": {
              "value": 89.58087379178397
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673060000,
            "doc_count": 6,
            "max_of_field": {
              "value": 88.39341429374308
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673120000,
            "doc_count": 6,
            "max_of_field": {
              "value": 84.27665178871197
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673180000,
            "doc_count": 6,
            "max_of_field": {
              "value": 84.95048030509422
            }
          },
          ...
```

By inspection, we can see that the results are (within floating point tolerance) identical. We have done this by hand for a representative selection of queries for each benchmark run.

Successful query validation implies that the benchmarking suite has end-to-end reproducibility, and is correct between both databases.
