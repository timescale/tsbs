import matplotlib.pyplot as plt
import numpy as np
import matplotlib.pyplot as plt

# query, % diff, plain min [ms], plain mean, plain max, ht min, ht mean, ht max
query_times = [('cpu-max-all-eight-hosts',5.95,24636.7,35724.14,56855.98,23180.64,33599.56,47687.26),\
               ('cpu-max-all-single-host',19.51,27.61,717.32,1788.11,31.62,577.39,1311.18),\
               ('groupby',68.92,47564.97,92368.51,158756.43,8322.97,28709.68,64471.76),\
               ('high-cpu-and-field',2.22,45.02,1050.97,2582.23,55.07,1027.61,2562.32),\
               ('high-cpu',1.60,12850.14,27368.16,39802.75,11289.32,26929.22,39665.32),\
               ('lastpoint (Only 5 runs)',1.31,348764.34,348982.27,349300.67,337359.23,344413.99,347810.75),\
               ('single-host',10.10,17.48,973.66,8085.21,23.58,875.35,5053.82)]

def make_query_barplot(query_data):
    plt.rcdefaults()
    fig, ax = plt.subplots()

    query_names = [q[0] for q in query_data]

    y_pos = np.arange(len(query_names))

    percentage_diff = [q[1] for q in query_data]

    ax.barh(y_pos, percentage_diff, align='center',
            color='green', ecolor='black')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(query_names)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel('Query latency difference [%]')

    plt.xlim([-20,100])
    plt.subplots_adjust(left=0.25, right=0.9, top=0.9, bottom=0.1)
    plt.show()

if __name__ == '__main__':
    make_query_barplot(query_times)