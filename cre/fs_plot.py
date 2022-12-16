import matplotlib as mpl
from matplotlib import pyplot as plt
import numpy as np
import fs_datatools as dt
import math
import statistics
import matplotlib.ticker as ticker

#given min, max and number of bins get centers
def get_bin_centers(xmin,xmax,nbins):
    delta = float(xmax-xmin)/float(nbins)
    return [xmin+(delta/2.0)+delta*i for i in range(0,nbins)] 


#given a list of lists get the minimum and maximum 
def maxmin_lists(data_lists):
    tot_min = float("Inf")
    tot_max = float("-Inf")
    for data_list in data_lists:
        if min(data_list) < tot_min:
            tot_min = min(data_list)
        if max(data_list) > tot_max:
            tot_max = max(data_list)
    return tot_max,tot_min

#get the number of items in each bin given a data list
def get_bin_counts(data_list,xmin,xmax,nbins):
    delta = float(xmax-xmin)/float(nbins)
    lefts = [item-(delta/2.0) for item in get_bin_centers(xmin,xmax,nbins)]
    rights = [item+(delta/2.0) for item in get_bin_centers(xmin,xmax,nbins)]
    counts = [ len([item for item in data_list if item >= lefts[i] and item < rights[i]]) for i in range(0,len(lefts))] 
    return counts

#given a list of data lists and number of bins overlay multiple histograms on the same plot. Options: N = Normalize, F = Fill, L = Logarithmic, E = Error Bars
def plot_hists(data_lists,labels,nbins=10,options="F",xtitle="",ytitle="Num Entries / Bin"):
    options = options.upper()
    top_ticks = []
    top_labels = []
    colors = ["red","blue","forestgreen","orchid","orange","brown","lightgray","cyan","pink","yellow","line","gold"]
    htype = "bar"
    log = 0
    normed = 0
    errors = 0
    if type(data_lists) == list:
        if not type(data_lists[0]) == list:
            data_lists = [data_lists]
    elif type(data_lists) != list:
        data_lists = [list(data_lists)]

    if not "F" in options:
       htype = "step"
    if "L" in options:
       log = 1
    if "N" in options:
       normed = 1	
    if "E" in options:
       errors = 1
    xmax,xmin = maxmin_lists(data_lists) 
    bins = np.linspace(xmin, xmax, nbins)
    bin_centers = get_bin_centers(xmin,xmax,nbins)
    for i in range(0,len(data_lists)):
        if not errors:
            plt.hist(data_lists[i], bins, alpha=0.5, label=labels[i],density = normed, histtype=htype, color=colors[i], log = log)
        else:
            y = get_bin_counts(data_lists[i],xmin,xmax,nbins) 
            y_rel_err = [float(y_i)**0.5/y_i if y_i > 0 else 0 for y_i in y]
            y_err = [float(y_i)**0.5 for y_i in y]

            if normed:
                y_err = [y_err[i]/y[i] if y_err[i] > 0 and y[i] > 0 else 0 for i in range(0,len(y_err))]

            if log:
                y = [abs(math.log(y_i)) if y_i > 0 else 0 for y_i in y]
                if not normed:
                   y_err = [y_rel_err[i]*y[i] for i in range(len(y_rel_err))]

            if normed:
                denom = float(sum(y)) 
                y = [item/denom for item in y]

            plt.errorbar(bin_centers, y, y_err,  marker = '.',  color=colors[i], drawstyle="steps",linewidth=1.5, label = labels[i],fillstyle="full") 

            if "F" in options:
                plt.hist(data_lists[i], bins, alpha=0.5, label=labels[i],density = normed, histtype=htype, color=colors[i], log = log)

        if "A" in options:
            avg = sum(data_lists[i])/len(data_lists[i])
            print("AVG:",avg,"STD",statistics.stdev(data_lists[i]))
            plt.axvline(x=avg,color='red',linestyle="--")
            ax = plt.gca()
            top_ticks.append(avg)
            top_labels.append("%.2f" % avg)

            ax.xaxis.set_minor_locator(ticker.FixedLocator(top_ticks))
            ax.xaxis.set_minor_formatter(ticker.FixedFormatter(top_labels))
            plt.gca().tick_params(axis="x", which="minor", labelrotation=90, 
                       top=1, bottom=0, labelbottom=0, labeltop=1) 
#    plt.legend(bbox_to_anchor=(0, 1), loc='upper left', ncol=1)


    #plt.legend(bbox_to_anchor=(1.04,1), borderaxespad=0)
    #plt.tight_layout(rect=[0,0,0.85,1])
    plt.legend(loc="upper left", bbox_to_anchor=(1,1))
    plt.subplots_adjust(right=0.6)
    plt.xlabel(xtitle)
    plt.ylabel(ytitle)
    plt.show()
