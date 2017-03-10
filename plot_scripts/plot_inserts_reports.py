import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import re
import numpy.polynomial.polynomial as poly
import argparse
import sys

logline_with_time_re =re.compile(r"REPORT: time (\d+) col rate (?P<col_rate>\d+\.\d+)/sec row rate (?P<row_rate>\d+\.\d+)/sec " \
                       r"\(period\) (?P<period>\d+\.\d+)/sec \(total\) total rows (?P<total_rows>\d+\.\d+[E0-9+]+)")

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
        plt.scatter(x, y, marker=style['marker'], color=style['color'], alpha=0.4)
        coefs = poly.polyfit(x, y, 3)
        ffit = poly.polyval(x, coefs)
        plt.plot(x, ffit, str_style.replace('*', '').replace('+', ''), lw=style['lw'], label=style['label'])
    else:
        plt.plot(x, y, str_style, label=style['label'], lw=style['lw'])


def smooth_it(x,y, downsample=100):

    slots = np.resize(y, (len(y) / downsample, downsample))

    smoothed_y = np.mean(slots, axis = 1)
    smoothed_x = np.arange(len(smoothed_y))

    return smoothed_x * downsample, smoothed_y



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', dest='batch_size', default=10000, type=int)
    parser.add_argument('-d', dest='dir', default='new_insert_no_debug', type=str)
    parser.add_argument('-p', dest='pretty', default=False, action='store_true')
    parser.add_argument('--trend', dest='trend', default=False, action='store_true')
    args = parser.parse_args()

    report_period = 20000
    # workers = 10
    # data_set_length = '5d'

    plain_dir = args.dir + '/plain'
    hyper_dir = args.dir + '/hypertable'

    if args.batch_size <= 0:
        print 'Batch size must be > 0'
        sys.exit(1);

    if True:
        plain = read_log_file('{}/load_plain_{}_{}.log'.format(plain_dir, args.batch_size, report_period))
        xplain, yplain = plain[:, 4], plain[:, 2]
        style = { 'line': '-', 'marker': '*', 'color': 'b', 'label': "plain", 'lw': 2 }
        plot_series(xplain, yplain, args.trend, style)
        plain_time = (plain[-1,0] - plain[0,0]) / 3600
    else:
        plain_time = 0

    hyper = read_log_file('{}/load_hypertable_{}_{}.log'.format(hyper_dir, args.batch_size, report_period))
    xhyper, yhyper = hyper[:, 4], hyper[:, 2]
    style = { 'line': '-', 'marker': '+', 'color': 'r', 'label': "hypertable", 'lw': 2 }
    plot_series(xhyper, yhyper, args.trend, style)
    hyper_time = (hyper[-1, 0] - hyper[0, 0]) / 3600


    if args.pretty:
        plt.title("Insert batch size: {}".format(args.batch_size))
    else:
        plt.title("Insert batch size: {}, plain {:.1f} h, hyper table {:.1f} h".format(args.batch_size, plain_time, hyper_time))

    plt.xlabel("Database size [millions of rows]")
    plt.ylabel("Insert rate [rows / second]")
    plt.ticklabel_format(style='sci', axis='y') #, scilimits=(0, 0))

    axes = plt.gca()
    axes.get_xaxis().set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x/1e6)))
    axes.set_xlim(left=0)
    axes.set_ylim(bottom=0)

    # 16:9
    scale=1.8
    plt.gcf().set_size_inches((16./scale), (9/scale))

    plt.legend(frameon=False)
    plt.savefig('write_performance_{}_{}.png'.format(args.batch_size,report_period))
    # plt.show()
