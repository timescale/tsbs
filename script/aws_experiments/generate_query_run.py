import argparse
import os

def get_load_str(load_file, label, batch_size, workers, reporting_period=20000):
    logfilename = 'load_{}_{}_{}.log'.format(label, batch_size, reporting_period)

    if label == "influxdb":
        return 'NUM_WORKERS={} DATA_DIR=/tmp BULK_DATA_DIR=/mnt/devops DATABASE_HOST=localhost BATCH_SIZE={} ./load_{}.sh | tee {}'.format(
            workers, batch_size, label, logfilename)
    elif label == "cassandra":
        return 'NUM_WORKERS={} DATA_DIR=/tmp BULK_DATA_DIR=/mnt/devops DATABASE_HOST=localhost CASSANDRA_BATCH_SIZE={} ./load_{}.sh | tee {}'.format(
            workers, batch_size, label, logfilename)


def get_query_str(queryfile, label, workers=10, limit=None):
    return 'cat {} | gunzip | query_benchmarker_{} -workers {} {} | tee {}'\
        .format(queryfile, label, workers,
                '-limit {}'.format(limit) if limit is not None else '',
                'query_{}_{}'.format(label, queryfile.split('/')[-1]).split('.')[0])

def load_queries_file_names(filename, label, query_dir='/mnt/queries'):
    l = list()
    with open(filename, 'r') as queries:
        for query in queries:
            query = query.split('#')[0]
            if len(query) > 0:
                l.append(os.path.join(query_dir, "{}-{}-queries.gz".format(label, query.strip())))

    return l

def generate_run_file(queries_file, load_file, db_name, batch_sizes, workers=8):

    print '#!/bin/bash'
    queries = None
    if queries_file is not None:
        queries = load_queries_file_names(queries_file, db_name)

    if load_file is not None:
        for batch_size in batch_sizes:
            print get_load_str(load_file, db_name, batch_size, workers)

    for query in queries:
        print get_query_str(query, db_name, workers=workers)


if __name__ == "__main__":
    default_load_file = '/mnt/devops/influx-bulk-data.gz'

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='batch_sizes_str', default="10000", type=str)
    parser.add_argument('-f', dest='queries_file_name', default='queries.txt', type=str)
    parser.add_argument('-l', dest='load_file_name', default=default_load_file, type=str)
    parser.add_argument('-w', dest='write_only', default=False, action='store_true')
    parser.add_argument('-d', dest='db_name', default=None, type=str)
    parser.add_argument('-q', dest='query_only', default=False, action='store_true')

    args = parser.parse_args()

    if args.db_name is None:
        print("Usage: generate_query_run.py -d db_name")
        exit(1)

    batch_sizes = [int(b) for b in args.batch_sizes_str.split(',')]

    generate_run_file(
        args.queries_file_name if not args.write_only else None,
        args.load_file_name if not args.query_only else None,
        args.db_name,
        batch_sizes)
