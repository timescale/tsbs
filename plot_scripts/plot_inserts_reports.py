import numpy as np
import matplotlib.pyplot as plt
import re
import numpy.polynomial.polynomial as poly

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

def plot_series(x, y, plot_fit, style, label):
    plt.plot(x, y, style, label=label)

    if plot_fit:
        coefs = poly.polyfit(x, y, 2)
        ffit = poly.polyval(x, coefs)
        plt.plot(x, ffit, style.replace('*', '').replace('+', ''))


def smooth_it(x,y, downsample=100):

    slots = np.resize(y, (len(y) / downsample, downsample))

    smoothed_y = np.mean(slots, axis = 1)
    smoothed_x = np.arange(len(smoothed_y))

    return smoothed_x * downsample, smoothed_y



if __name__ == '__main__':

    add_trend = True
    batch_size = 10000

    report_period = 20000
    data_set_length = '5d'
    workers = 10

    dir = 'new_insert_no_debug'

    if True:
        plain = read_log_file('{}/load_plain_{}_{}.log'.format('plain', batch_size, report_period))
        xplain, yplain = plain[:, 4], plain[:, 2]
        plot_series(xplain, yplain, add_trend, '-*b', "plain")
        plain_time = (plain[-1,0] - plain[0,0]) / 3600
    else:
        plain_time = 0

    hyper = read_log_file('{}/load_hypertable_{}_{}.log'.format(dir, batch_size, report_period))
    xhyper, yhyper = hyper[:, 4], hyper[:, 2]

    plot_series(xhyper, yhyper, add_trend, '-+r', "hypertable")
    hyper_time = (hyper[-1, 0] - hyper[0, 0]) / 3600

    plt.title("Batch size: {}, plain {:.1f} h, hyper table {:.1f} h".format(batch_size, plain_time, hyper_time))
    plt.xlabel("rows")
    plt.ylabel("Rate [rows/s]")
    plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))

    plt.legend()
    plt.savefig('write_performance_{}_{}.png'.format(batch_size,report_period))
    plt.show()
