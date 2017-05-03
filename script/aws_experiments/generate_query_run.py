'''Generates bash scripts to run benchmarks for a variety of databases.

This script generates a bash script that can be run as part of a benchmarking
pipline. The output of the script relies on load_* scripts in the same directory
for inserting, and on Go binaries generated from Influx's benchmark suite.

Usage flags:
    -b      Sets the batch size for inserting (default: 10000)

    -d      Database to benchmark. Valid values: cassandra, influxdb, timescaledb

    -f      Filename containing query benchmark names, one per line. Lines can
            be commented out to exclude those benchmarks (default: queries.txt)

    -i      Insert benchmark only, no queries (default: false)

    -l      Directory where input files are located. They should be named
            `[database]-data.gz` (default: /tmp)

    -o      Directory where query files are located (default: /tmp/queries)

    -q      Query benchmarks only, no insert. Data needs to be previously
            inserted (default: false)

    -w      Number of workers/threads to use (default: 4)

EXAMPLE:

queries.txt:
#1-host-12-hr
5-metrics-1-host-1-hr
5-metrics-1-host-12-hr

Command:
python generate_query_run -d timescaledb -w 8

Result:
#!/bin/bash
# Load data
NUM_WORKERS=8 DATA_DIR=/tmp BULK_DATA_DIR=/tmp DATABASE_HOST=localhost BATCH_SIZE=10000 ./load_timescaledb.sh | tee load_timescaledb_10000_20000.out

# Queries
cat /tmp/queries/timescaledb-5-metrics-1-host-1-hr-queries.gz | gunzip | query_benchmarker_timescaledb -workers 8 -limit 1000 -postgres "host=localhost user=postgres sslmode=disable database=benchmark timescaledb.disable_optimizations=false" | tee query_timescaledb_timescaledb-5-metrics-1-host-1-hr-queries.out

cat /tmp/queries/timescaledb-5-metrics-1-host-12-hr-queries.gz | gunzip | query_benchmarker_timescaledb -workers 8 -limit 1000 -postgres "host=localhost user=postgres sslmode=disable database=benchmark timescaledb.disable_optimizations=false" | tee query_timescaledb_timescaledb-5-metrics-1-host-12-hr-queries.out
'''
import argparse
import os

def get_load_str(load_dir, label, batch_size, workers, reporting_period=20000):
    logfilename = 'load_{}_{}_{}.out'.format(label, batch_size, reporting_period)
    prefix = 'NUM_WORKERS={} DATA_DIR=/tmp BULK_DATA_DIR={} DATABASE_HOST=localhost'.format(workers, load_dir)
    suffix = ' ./load_{}.sh | tee {}'.format(label, logfilename)

    if label == 'influxdb':
        return prefix + ' BATCH_SIZE={}'.format(batch_size) + suffix
    elif label == 'cassandra':
        return prefix + ' CASSANDRA_BATCH_SIZE={}'.format(batch_size) + suffix
    elif label == 'timescaledb':
        return prefix + ' BATCH_SIZE={}'.format(batch_size) + suffix


def get_query_str(queryfile, label, workers, limit=1000):
    extra_args = ''
    if label == 'cassandra':
        extra_args = '-aggregation-plan client'
    elif label == 'timescaledb':
        extra_args = '-postgres "{}"'.format('host=localhost user=postgres sslmode=disable database=benchmark timescaledb.disable_optimizations=false')

    limit_arg = '-limit {}'.format(limit) if limit is not None else ''
    output_file = 'query_{}_{}'.format(label, queryfile.split('/')[-1]).split('.')[0]

    return 'cat {} | gunzip | query_benchmarker_{} -workers {} {} {} | tee {}.out'.format(
        queryfile, label, workers, limit_arg, extra_args, output_file)

def load_queries_file_names(filename, label, query_dir):
    l = list()
    with open(filename, 'r') as queries:
        for query in queries:
            query = query.split('#')[0]
            if len(query) > 0:
                n = label if label != "influxdb" else "influx-http"
                l.append(os.path.join(query_dir, "{}-{}-queries.gz".format(n, query.strip())))

    return l

def generate_run_file(queries_file, query_dir, load_dir, db_name, batch_sizes, workers):

    print '#!/bin/bash'
    queries = None
    if queries_file is not None:
        queries = load_queries_file_names(queries_file, db_name, query_dir)

    if load_dir is not None:
        for batch_size in batch_sizes:
            print("# Load data")
            print(get_load_str(load_dir, db_name, batch_size, workers))
            print("")

    if queries is not None:
        print("# Queries")
    for query in queries:
        print(get_query_str(query, db_name, workers))
        print("")


if __name__ == "__main__":
    default_load_dir = '/tmp'
    default_query_dir = '/tmp/queries'

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='batch_sizes_str', default="10000", type=str)
    parser.add_argument('-d', dest='db_name', default=None, type=str)
    parser.add_argument('-f', dest='queries_file_name', default='queries.txt', type=str)
    parser.add_argument('-i', dest='write_only', default=False, action='store_true')
    parser.add_argument('-l', dest='load_file_dir', default=default_load_dir, type=str)
    parser.add_argument('-o', dest='query_file_dir', default=default_query_dir, type=str)
    parser.add_argument('-q', dest='query_only', default=False, action='store_true')
    parser.add_argument('-w', dest='workers', default=4, type=int)

    args = parser.parse_args()

    if args.db_name is None:
        print("Usage: generate_query_run.py -d db_name")
        exit(1)

    batch_sizes = [int(b) for b in args.batch_sizes_str.split(',')]

    generate_run_file(
        args.queries_file_name if not args.write_only else None,
        args.query_file_dir if not args.write_only else None,
        args.load_file_dir if not args.query_only else None,
        args.db_name,
        batch_sizes,
        args.workers)
