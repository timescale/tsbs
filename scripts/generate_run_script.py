'''Generates bash scripts to run benchmarks for a variety of databases.

This script generates a bash script that can be run as part of a benchmarking
pipline. The output of the script relies on load_* scripts in the same directory
for inserting, and on Go binaries generated from Influx's benchmark suite.

Usage flags:
    -b      Sets the batch size for inserting (default: 10000)

    -d      Database to benchmark. Valid values: cassandra, influx, timescaledb, postgres

    -e      Extra flags to pass to pass to query benchmarker, e.g., "-show-explain -debug 9"

    -f      Filename containing query benchmark names, one per line. Lines can
            be commented out to exclude those benchmarks (default: queries.txt)

    -i      Insert benchmark only, no queries (default: false)

    -l      Directory where input files are located. They should be named
            `[database]-data.gz` (default: /tmp)
            
    -n      Number of queries to run for each query type (default: 1000)

    -o      Directory where query files are located (default: /tmp/queries)

    -q      Query benchmarks only, no insert. Data needs to be previously
            inserted (default: false)

    -s      Hostname for the client to connect to (default: localhost)

    -w      Number of workers/threads to use (default: 4)

EXAMPLE:

queries.txt:
#single-groupby-1-1-1
single-groupby-5-1-1
single-groupby-5-1-12

Command:
python generate_query_run -d timescaledb -w 8

Result:
#!/bin/bash
# Load data
NUM_WORKERS=8 BULK_DATA_DIR=/tmp DATABASE_HOST=localhost BATCH_SIZE=10000 ./load_timescaledb.sh | tee load_timescaledb_8_10000.out

# Queries
cat /tmp/queries/timescaledb-single-groupby-5-1-1-queries.gz | gunzip | tsbs_run_queries_timescaledb -workers 8 -max-queries 1000 -postgres "host=localhost user=postgres sslmode=disable" | tee query_timescaledb_timescaledb-single-groupby-5-1-1-queries.out

cat /tmp/queries/timescaledb-single-groupby-5-1-12-queries.gz | gunzip | tsbs_run_queries_timescaledb -workers 8 -max-queries 1000 -postgres "host=localhost user=postgres sslmode=disable" | tee query_timescaledb_timescaledb-single-groupby-5-1-12-queries.out
'''
import argparse
import os

def get_load_str(load_dir, label, batch_size, workers, hostname):
    '''Writes a script line corresponding to loading data into a database'''
    logfilename = 'load_{}_{}_{}.out'.format(label, workers, batch_size)
    prefix = 'NUM_WORKERS={} BATCH_SIZE={} BULK_DATA_DIR={} DATABASE_HOST={}'.format(workers, batch_size, load_dir, hostname)

    loader = label if label != 'postgres' else 'timescaledb'
    suffix = ' ./load_{}.sh | tee {}'.format(loader, logfilename)

    if label == 'influx' or label == 'cassandra':
        return prefix + suffix
    elif label == 'timescaledb':
        return prefix + ' USE_HYPERTABLE=true ' + suffix
    elif label == "postgres":
        return prefix + ' USE_HYPERTABLE=false ' + suffix


def get_query_str(queryfile, label, workers, limit, hostname, extra_query_args):
    '''Writes a script line corresponding to executing a query on a database'''
    limit_arg = '--max-queries={}'.format(limit) if limit is not None else ''
    output_file = 'query_{}_{}'.format(label, queryfile.split('/')[-1]).split('.')[0]
    benchmarker = label if label != 'postgres' else 'timescaledb'

    extra_args = ''
    if label == 'cassandra':
        # Cassandra has an extra option to choose between server & client
        # aggregation plans. Client seems to be better in all cases
        extra_args = '--aggregation-plan=client'
    elif label == 'timescaledb' or label == 'postgres':
        # TimescaleDB needs the connection string
        extra_args = '--hosts="{}" --postgres="{}"'.format(hostname, 'user=postgres sslmode=disable')

    return 'cat {} | gunzip | tsbs_run_queries_{} --workers={} {} {} {} | tee {}.out'.format(
        queryfile, benchmarker, workers, limit_arg, extra_args, extra_query_args, output_file)

def load_queries_file_names(filename, label, query_dir):
    '''Gets the list of files containing benchmark queries'''

    l = list()
    with open(filename, 'r') as queries:
        for query in queries:
            query = query.split('#')[0]
            if len(query) > 0:
                n = label
                if label == 'postgres':
                    n = 'timescaledb'
                l.append(os.path.join(query_dir, "{}-{}-queries.gz".format(n, query.strip())))

    return l

def generate_run_file(queries_file, query_dir, load_dir, db_name, batch_size, limit, workers, hostname, extra_query_args):
    '''Writes a bash script file to run load/query tests'''

    print('#!/bin/bash')
    queries = []
    if queries_file is not None:
        queries = load_queries_file_names(queries_file, db_name, query_dir)

    if load_dir is not None:
        print("# Load data")
        print(get_load_str(load_dir, db_name, batch_size, workers, hostname))
        print("")

    if len(queries) > 0:
        print("# Queries")
        for query in queries:
            print(get_query_str(query, db_name, workers, limit, hostname, extra_query_args))
            print("")


if __name__ == "__main__":
    default_load_dir = '/tmp'
    default_query_dir = '/tmp/queries'

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='batch_size', default=10000, type=int,
        help='Batch size for inserts')
    parser.add_argument('-d', dest='db_name', default=None, required=True,
        type=str, help='Database to generate commands for')
    parser.add_argument('-e', dest='extra_query_args', default='', type=str,
        help='Extra arguments to pass directly to query runner')
    parser.add_argument('-f', dest='queries_file_name', default='queries.txt',
        type=str, help='File containing a list of queries to run, one per line')
    parser.add_argument('-i', dest='write_only', default=False,
        action='store_true', help='Whether to only generate commands for inserts')
    parser.add_argument('-l', dest='load_file_dir', default=default_load_dir,
        type=str, help='Path to directory where data to insert is stored')
    parser.add_argument('-n', dest='limit', default=1000, type=int,
        help='Max number of queries to run')
    parser.add_argument('-o', dest='query_file_dir', default=default_query_dir,
        type=str, help='Path to directory where queries to execute are stored')
    parser.add_argument('-q', dest='query_only', default=False,
        action='store_true', help='Whether to only generate commands for queries')
    parser.add_argument('-s', dest='hostname', default='localhost',
        type=str, help='Hostname of the database')
    parser.add_argument('-w', dest='workers', default=4, type=int,
        help='Number of workers to use for inserts and queries')

    args = parser.parse_args()

    if args.db_name is None:
        print("Usage: generate_query_run.py -d db_name")
        exit(1)

    generate_run_file(
        args.queries_file_name if not args.write_only else None,
        args.query_file_dir if not args.write_only else None,
        args.load_file_dir if not args.query_only else None,
        args.db_name,
        args.batch_size,
        args.limit,
        args.workers,
        args.hostname,
        args.extra_query_args)
