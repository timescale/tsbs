# TSBS Supplemental Guide: RedisTimeSeries

RedisTimeSeries is a Redis Module adding a Time Series data structure to Redis.
The tsbs benchmark suite currently has two use cases, DevOps and IoT. 
This supplemental guide focus on RedisTimeSeries DevOps support. 

**Note:** Within the Devops use-case RedisTimeSeries does not support the `high-cpu-1` and `high-cpu-all` queries,
which should soon be supported. 
This means that on total the RedisTimeSeries query performance will be focused on 13 out of the total 15 tsbs DevOps available queries.

This supplemental guide explains:
- How to build the required Go programs
- How to generate the required datasets and queries
- How to load the data into RedisTimeSeries
- How to benchmark RedisTimeSeries query performance

**This should be read *after* the main README.**


---

## Installation

TSBS is a collection of Go programs (with some auxiliary bash and Python
scripts). The easiest way to get and install the Go programs is to use
`go get` and then `go install`:
```bash
# Fetch TSBS and its dependencies
$ go get github.com/timescale/tsbs
$ cd $GOPATH/src/github.com/timescale/tsbs/cmd
$ go get ./...

# Install redistimeseries binaries. 
cd $GOPATH/src/github.com/timescale/tsbs
make
```

# benchmark commands
```
# generate the dataset 
FORMATS="redistimeseries" SCALE=100 SEED=123 \
    scripts/generate_data_redistimeseries.sh

# generate the queries
FORMATS="redistimeseries" SCALE=100 SEED=123 \
    scripts/generate_queries_redistimeseries.sh

# load the data into RedisTimeSeries
SCALE=100 scripts/load/load_redistimeseries.sh

# benchmark RedisTimeSeries query performance
SCALE=100 scripts/run_queries/run_queries_redistimeseries.sh
```
