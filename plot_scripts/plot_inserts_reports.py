import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import re
import numpy.polynomial.polynomial as poly
import argparse
import sys

logline_with_time_re =re.compile(r"REPORT: time (\d+) col rate (?P<col_rate>\d+\.\d+)/sec row rate (?P<row_rate>\d+\.\d+)/sec " \
                       r"\(period\) (?P<period>\d+\.\d+)/sec \(total\) total rows (?P<total_rows>\d+\.\d+[E0-9+]+)")

report_period = 20000

def read_log_file(filename, regexp=logline_with_time_re):

    with open(filename, 'rb') as datafile:
        rows = list()

        for row in datafile.readlines():
            m = regexp.match(row)

            if m is not None:
                rows.append([float(s) for s in m.groups()])

    return np.array(rows)

def plot_series(x, y, plot_trend, style):
    str_style = "%s%s%s" % (style['line'], style['marker'], style['color'])

    if plot_trend:
        plt.scatter(x, y, marker=style['marker'], color=style['color'], alpha=style['alpha'])
        coefs = poly.polyfit(x, y, 3)
        ffit = poly.polyval(x, coefs)
        plt.plot(x, ffit, str_style.replace('*', '').replace('+', ''), lw=style['lw'], label=style['label'])
    else:
        #plt.plot(x, y, str_style, label=style['label'], lw=style['lw'])
        plt.scatter(x, y, marker=style['marker'], color=style['color'], alpha=style['alpha'], label=style['label'])


def smooth_it(x,y, downsample=100):

    slots = np.resize(y, (len(y) / downsample, downsample))

    smoothed_y = np.mean(slots, axis = 1)
    smoothed_x = np.arange(len(smoothed_y))

    return smoothed_x * downsample, smoothed_y


def plot_plain(args):
    plain_dir = args.dir + '/plain'

    if True:
        plain = read_log_file('{}/load_plain_{}_{}.log'.format(plain_dir, args.batch_size, report_period))
        xplain, yplain = plain[:, 4], plain[:, 2]
        alpha = 0.5 if args.trend else 0.8
        style = { 'line': '-', 'marker': '+', 'color': 'b', 'label': "PostgreSQL", 'lw': 3, 'alpha': alpha }
        plot_series(xplain, yplain, args.trend, style)
        plain_time = (plain[-1,0] - plain[0,0]) / 3600
    else:
        plain_time = 0

    return plain_time


def plot_both(args):

    hyper_dir = args.dir + '/hypertable'

    plain_time = plot_plain(args)

    alpha = 0.3 if args.batch_size < 1000 else 0.6 if args.trend else 9

    hyper = read_log_file('{}/load_hypertable_{}_{}.log'.format(hyper_dir, args.batch_size, report_period))
    xhyper, yhyper = hyper[:, 4], hyper[:, 2]
    style = { 'line': '-', 'marker': '*', 'color': 'r', 'label': "TimescaleDB", 'lw': 3, 'alpha': alpha}
    plot_series(xhyper, yhyper, args.trend, style)
    hyper_time = (hyper[-1, 0] - hyper[0, 0]) / 3600

    if not args.pretty:
        plt.title("Insert batch size: {}, Cache: {} GB memory, plain {:.1f} h, hyper table {:.1f} h".format(args.batch_size, args.memory, plain_time, hyper_time))


def generate_figure(args, desc):

    plt.xlabel("Dataset size [millions of rows]")
    plt.ylabel("Insert rate [rows / second]")
    plt.ticklabel_format(style='plain', axis='y') #, scilimits=(0, 0))

    axes = plt.gca()
    axes.get_xaxis().set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x/1e6)))
    axes.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    axes.set_xlim(left=0, right=270000000)
    axes.set_ylim(bottom=0)

    if args.pretty:
        plt.title("Insert batch size: {},  Cache: {} GB memory".format(args.batch_size, args.memory))

    # 16:9
    scale=1.8
    plt.gcf().set_size_inches((16./scale), (9/scale))

    if args.trend:
        plt.legend(frameon=False)
    else:
        plt.legend(frameon=False, numpoints=3)

    #plt.savefig('write_performance_{}_{}_{}-{}.png'.format(args.batch_size, report_period, args.memory, desc), dpi=600, transparent=True)
    plt.savefig('write_performance_{}_{}_{}-{}.pdf'.format(args.batch_size, report_period, args.memory, desc), format='pdf')
    # plt.show()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='batch_size', default=10000, type=int)
    parser.add_argument('-d', dest='dir', default='new_insert_no_debug', type=str)
    parser.add_argument('-p', dest='pretty', default=False, action='store_true')
    parser.add_argument('-m', dest='memory', default=16, type=int)
    parser.add_argument('--trend', dest='trend', default=False, action='store_true')
    args = parser.parse_args()


    # workers = 10
    # data_set_length = '5d'

    if args.batch_size <= 0:
        print 'Batch size must be > 0'
        sys.exit(1);

    plot_both(args)
    generate_figure(args, "both")
    plt.clf()
    plot_plain(args)
    generate_figure(args, "plain")



