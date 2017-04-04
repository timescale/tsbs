import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# query, % diff, plain min [ms], plain mean, plain max, ht min, ht mean, ht max
#('cpu-max-all-eight-hosts',
#('cpu-max-all-single-host'
#('groupby'
#('high-cpu-and-field'
#('high-cpu'
#('lastpoint (Only 5 runs)'
#('single-host'

query_times = [('high vals for 1 device\n over time interval',2.27,45.02,1050.97,2582.23,55.07,1027.61,2562.32),\
#               ('high vals for all devices',1.60,12850.14,27368.16,39802.75,11289.32,26929.22,39665.32),\
#               ('latest per device',1.31,348764.34,348982.27,349300.67,337359.23,344413.99,347810.75),\
               ('max per min for 1 device\n over time interval',11.23,17.48,973.66,8085.21,23.58,875.35,5053.82),\
               ('max per hour for 1 device\n over time interval',24.23,27.61,717.32,1788.11,31.62,577.39,1311.18),\
#               ('max per hour for 8 devices',6.32,24636.7,35724.14,56855.98,23180.64,33599.56,47687.26),\
               ('avg per hour per every device \n over time interval',221.73,47564.97,92368.51,158756.43,8322.97,28709.68,64471.76),\
               ('max per min across all devices \n with limit',454.68,4658.80, 6367.10, 8556.71,23.85, 1147.88,2952.15)]

def make_query_barplot(query_data):
    plt.rcdefaults()
    fig, ax = plt.subplots()

    query_names = [q[0] for q in query_data]

    y_pos = np.arange(len(query_names))

    percentage_diff = [q[1] for q in query_data]

    ax.barh(y_pos, percentage_diff, align='center',
            color='blue', ecolor='black', height=0.6)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(query_names)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel('Query latency improvement [%]')
    ax.xaxis.set_major_locator(ticker.MultipleLocator(50))
    plt.xlim([-20,510])

    rects = ax.patches

    # Now make some labels
    labels = ["%d" % q[1] for q in query_data]

    for rect, label in zip(rects, labels):
        ax.text(rect.get_width() + 8, rect.get_y()+.3, label+"%", ha='left', va='center')

    # 16:9
    scale=1.3
    plt.gcf().set_size_inches((16./scale), (9/scale))

    plt.subplots_adjust(left=0.3, right=.9, top=0.6, bottom=0.1)
    plt.savefig('query_performance.pdf', format='pdf')
    #plt.savefig('query_performance.png', dpi=300, transparent=True)
    #plt.show()

if __name__ == '__main__':
    make_query_barplot(query_times)
