import argparse

def get_load_str(hypertable, load_file, batch_size, chunk_size, workers, num_partitions, reporting_period=20000,
                 postgresstr='host=localhost user=postgres sslmode=disable'
                 ):

    if hypertable:
        table_type = 'hypertable'
    else:
        table_type = 'plain'

    logfilename = 'load_{}_{}_{}.log'.format(table_type, batch_size, reporting_period)

    return 'cat {} | gunzip | ' \
           'bulk_load_timescaledb  -batch-size {} ' \
           '-field-index=VALUE-TIME -field-index-count=1 ' \
           '-make-hypertable={} -number_partitions {} -chunk-size {} -workers {} -postgres "{}" -reporting-period {} ' \
           '| tee {}'\
            .format(load_file,
                    batch_size,
                    'true' if hypertable else 'false',
                    num_partitions,
                    chunk_size,
                    workers,
                    postgresstr,
                    reporting_period,
                    logfilename)

def get_query_str(queryfile, label, workers=10, limit=None,
                  postgresstr='host=localhost user=postgres sslmode=disable database=benchmark'):
    postgresstr += " timescaledb.disable_optimizations={}".format('false' if label is 'hypertable' else 'true')
    return 'cat {} | gunzip | query_benchmarker_timescaledb -workers {} -postgres "{}" {} | tee {}'\
        .format(queryfile, workers, postgresstr, '-limit {}'.format(limit) if limit is not None else '',
                'query_{}_{}'.format(label, queryfile.split('/')[-1]).split('.')[0])

def get_dump_oids_str(name):
    return 'psql -U postgres -d benchmark -h localhost  -A -F"," -c "select relname,relnamespace,oid from pg_class" > pg_class_oid_{}.csv'.format(name)


def load_queries_file_names(filename):
    l = list()
    with open(filename, 'r') as queries:
        for query in queries:
            query = query.split('#')[0]
            if len(query) > 0:
                l.append(query.strip())

    return l

def generate_run_file(queries_file, load_file, num_partitions, chunk_size, batch_sizes, workers=10):

    print '#!/bin/bash'
    queries = None
    if queries_file != None:
        queries = load_queries_file_names(queries_file)

    for hypertable in [True, False]:
        for batch_size in batch_sizes:
            print get_load_str(hypertable, load_file, batch_size, chunk_size, workers, num_partitions)
            #print get_dump_oids_str(batch_size)

            for query in queries:
                print get_query_str(query, 'hypertable' if hypertable else 'plain')


if __name__ == "__main__":

    default_load_file = '/benchmark_data/import_data/import_iobeam_528198289a8aaa955ba43b88d00c4a9d_10s_' \
                        '1000_123_2016-01-01T00\:00\:00Z_2016-02-01T00\:00\:00Z_cpu-only.dat.gz'

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='batch_sizes_str', default="1,10,10000", type=str)
    parser.add_argument('-f', dest='queries_file_name', default='queries.txt', type=str)
    parser.add_argument('-l', dest='load_file_name', default=default_load_file, type=str)
    parser.add_argument('-p', dest='num_partitions', default=1, type=int)
    parser.add_argument('-c', dest='chunk_size', default=1024*1024*1024, type=int)
    parser.add_argument('-w', dest='write_only', default=False, action='store_true')

    args = parser.parse_args()

    batch_sizes = [int(b) for b in args.batch_sizes_str.split(',')]

    generate_run_file(args.queries_file_name if not args.write_only else None, args.load_file_name,
                      args.num_partitions, args.chunk_size, batch_sizes)
