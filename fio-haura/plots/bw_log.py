#!/bin/env python

import numpy
import pandas
import matplotlib.pyplot as plt
import sys
import json
import glob

def plot_bw_lat_log(path):
    """
    Plot an amalgation of different plots containing bandwidth, latency and IOPS
    over time. This plots for each job a line, although they remain unnamed in
    the output.
    """
    bws = [pandas.read_csv(res, names=['msec', 'value', 'data_dir', 'bs', 'prio']) for res in glob.glob(path + '/bench_bw.*')]
    lats = [pandas.read_csv(res, names=['msec', 'value', 'data_dir', 'bs', 'prio']) for res in glob.glob(path + '/bench_lat.*')]
    iopss = [pandas.read_csv(res, names=['msec', 'value', 'data_dir', 'bs', 'prio']) for res in glob.glob(path + '/bench_iops.*')]

    fig, axs = plt.subplots(3,1,figsize=(6,7))
    # plot in MiB/s
    for bw in bws:
        axs[0].plot(bw['msec'] / 1000, bw['value'] / 1024)
    axs[0].set_title(f"{path} - Bandwidth [MiB/s]")
    axs[0].set_yscale('log')
    # plot in ns
    for lat in lats:
        axs[1].plot(lat['msec'] / 1000, lat['value'], label='Latency')
    axs[1].set_title(f"{path} - Average Latency [ns]")
    axs[1].set_yscale('log')
    # plot in IOPS
    for iops in iopss:
        axs[2].plot(iops['msec'] / 1000, iops['value'], label='IOPS')
    axs[2].set_title(f"{path} - IOPS [#]")
    axs[2].set_xlabel('Runtime [s]')
    axs[2].set_yscale('log')
    fig.tight_layout()
    fig.savefig(f'{path}/log.svg')

def plot_lat_dist(path):
    """
    Plot the latency distribution for completion latency (clat) from the fio
    output, this works regardless of grouped reporting or single job reporting.
    Although grouped reporting improves readability.
    """
    with open(path + '/output.json') as data:
        js = json.load(data)

    fig, ax = plt.subplots(1,1)
    total_jobs = len(js["jobs"])
    for (idx, job) in enumerate(js["jobs"]):
        bins = job["write"]["clat_ns"]["percentile"].keys()
        vals = job["write"]["clat_ns"]["percentile"].values()
        ax.bar(numpy.array(range(0,len(vals))) + 1/total_jobs * idx, vals, min(1/total_jobs, 0.8))
    ax.set_xticks(range(0,len(vals)), labels=[s[:5] for s in bins], rotation='vertical')
    ax.set_xlabel("Percentile [%]")
    ax.set_ylabel("Latency [ns]")
    ax.set_yscale('log')
    ax.set_title(f'{path} - Latencies')
    fig.tight_layout()
    fig.savefig(f'{path}/latency.svg')


if len(sys.argv) < 2:
    print("Usage:")
    print(f"    {sys.argv[0]} <RESULT> [<MORE_RESULTS>]")

for res in sys.argv[1:]:
    plot_bw_lat_log(res)
    plot_lat_dist(res)
