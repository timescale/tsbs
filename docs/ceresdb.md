# Build
```bash
# build generate data tool
go build ./cmd/tsbs_generate_data/

# build write tool
go build ./cmd/tsbs_load_ceresdb

# build query tool
go build ./cmd/tsbs_run_queries_ceresdb/
```

# Usage

## Write
```

./tsbs_generate_data --use-case="devops" --seed=123 --scale=10 \
      --timestamp-start="2022-09-05T00:00:00Z" \
      --timestamp-end="2022-09-05T00:00:10Z" \
      --log-interval="10s" --format="ceresdb" > data.input

./tsbs_load_ceresdb --file data.input
```

## Query

```
./tsbs_generate_queries --use-case="devops" --seed=123 --scale=2 \
      --timestamp-start="2022-09-05T00:00:00Z" \
      --timestamp-end="2022-09-05T12:00:10Z" \
      --queries=100 --query-type="single-groupby-1-1-12" --format="ceresdb" > query


./tsbs_run_queries_ceresdb --file query
```
Output

```
After 100 queries with 1 workers:
Interval query rate: 168.80 queries/sec Overall query rate: 168.80 queries/sec
CeresDB 1 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:     4.42ms, med:     5.11ms, mean:     5.91ms, max:   31.36ms, stddev:     3.72ms, sum:   0.6sec, count: 100
all queries                                                     :
min:     4.42ms, med:     5.11ms, mean:     5.91ms, max:   31.36ms, stddev:     3.72ms, sum:   0.6sec, count: 100

Run complete after 100 queries with 1 workers (Overall query rate 168.03 queries/sec):
CeresDB 1 cpu metric(s), random    1 hosts, random 12h0m0s by 1m:
min:     4.42ms, med:     5.11ms, mean:     5.91ms, max:   31.36ms, stddev:     3.72ms, sum:   0.6sec, count: 100
all queries                                                     :
min:     4.42ms, med:     5.11ms, mean:     5.91ms, max:   31.36ms, stddev:     3.72ms, sum:   0.6sec, count: 100
wall clock time: 0.598387sec

```
