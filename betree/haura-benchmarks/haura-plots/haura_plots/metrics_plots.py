"""
Plots visualizing the metrics produced by Haura.
"""
from . import util
import numpy as np
import matplotlib.pyplot as plt


# Formatting
def ms_to_string(time):
    """Nicer formatter for epoch strings in figures"""
    return f"{int(time / 1000 / 60)}:{int(time / 1000) % 60:02d}"

def plot_throughput(data, path):
    """
    Print a four row throughput plot with focussed read or write throughput.
    """

    epoch = [temp['epoch_ms'] for temp in data]
    util.subtract_first_index(epoch)
    epoch_formatted = list(map(ms_to_string, epoch))
    num_tiers = len(data[0]['storage']['tiers'])
    fig, axs = plt.subplots(num_tiers, 1, figsize=(16,8))
    for tier_id in range(num_tiers):
        for disk_id in range(len(data[0]['storage']['tiers'][tier_id]['vdevs'])):
            writes = np.array([])
            reads = np.array([])
            for point in data:
                writes = np.append(writes, point['storage']['tiers'][tier_id]['vdevs'][disk_id]['written'])
                reads = np.append(reads, point['storage']['tiers'][tier_id]['vdevs'][disk_id]['read'])

            if len(writes) > 0:
                util.subtract_last_index(writes)
                util.subtract_last_index(reads)

            # convert to MiB from Blocks
            # NOTE: We assume here a block size of 4096 bytes as this is the
            # default haura block size if you change this you'll need to modify
            # this here too.
            writes = writes * util.BLOCK_SIZE / 1024 / 1024 * (util.SEC_MS / util.EPOCH_MS)
            reads = reads * util.BLOCK_SIZE / 1024 / 1024 * (util.SEC_MS / util.EPOCH_MS)

        axs[tier_id].plot(epoch, reads, label = 'Read', linestyle='dotted', color=util.GREEN)
        axs[tier_id].plot(epoch, writes, label = 'Written', color=util.BLUE)
        axs[tier_id].set_xlabel("runtime (minute:seconds)")
        axs[tier_id].set_xticks(epoch, epoch_formatted)
        axs[tier_id].locator_params(tight=True, nbins=10)
        axs[tier_id].set_ylabel(f"{util.num_to_name(tier_id)}\nMiB/s (I/0)")
        label=' | '.join(path.split('/')[-2:])
    fig.legend(loc="center right",handles=axs[0].get_lines())
    # Epoch in seconds
    fig.suptitle(f"Haura - {label}", y=0.98)  # add title
    fig.savefig(f"{path}/plot_write.svg")
    for tier_id in range(num_tiers):
        lines = axs[tier_id].get_lines()
        if len(lines) > 0:
            lines[0].set_linestyle('solid')
            lines[0].zorder = 2.1
            lines[1].set_linestyle('dotted')
            lines[1].zorder = 2.0
    fig.legend(loc="center right",handles=axs[0].get_lines())
    fig.savefig(f"{path}/plot_read.svg")
    plt.close(fig)

def plot_tier_usage(data, path):
    """
    Plot the utilized space of each storage tier.
    """
    fig, axs = plt.subplots(4, 1, figsize=(10,13))

    # 0 - 3; Fastest - Slowest
    free = [[], [], [], []]
    total = [[], [], [], []]
    # Map each timestep to an individual
    for ts in data:
        tier = 0
        for stat in ts["usage"]:
            free[tier].append(stat["free"])
            total[tier].append(stat["total"])
            tier += 1

    tier = 0
    for fr in free:
        axs[tier].plot((np.array(total[tier]) - np.array(fr)) * 4096 / 1024 / 1024 / 1024, label="Used", marker="o", markevery=200, color=util.BLUE)
        axs[tier].plot(np.array(total[tier]) * 4096 / 1024 / 1024 / 1024, label="Total", marker="^", markevery=200, color=util.GREEN)
        axs[tier].set_ylim(bottom=0)
        axs[tier].set_ylabel(f"{util.num_to_name(tier)}\nCapacity in GiB")
        tier += 1

    fig.legend(loc='center right',handles=axs[0].get_lines())
    fig.savefig(f"{path}/tier_usage.svg")
    plt.close(fig)


def plot_system(path):
    """Plot the system usage and temperatures during the run."""
    data = []
    with open(f"{path}/out.jsonl", 'r', encoding="UTF-8") as metrics:
        data = util.read_jsonl(metrics)

    epoch = [temp['epoch_ms'] for temp in data]
    util.subtract_first_index(epoch)
    epoch_formatted = list(map(ms_to_string, epoch))
    min_pagefaults = [x["proc_minflt"] + x["proc_cminflt"] for x in data]
    maj_pagefaults = [x["proc_majflt"] + x["proc_cmajflt"] for x in data]
    virtual_mem = [x["proc_vsize"] for x in data]
    resident_mem = [x["proc_rss"] for x in data]
    utime = [x["proc_utime"] + x["proc_cutime"] for x in data]
    stime = [x["proc_stime"] + x["proc_cstime"] for x in data]

    fig, axs = plt.subplots(4,2, figsize=(10,13))
    eticks = range(0, epoch[-1:][0], 30 * 10**3);
    eticks_formatted = list(map(ms_to_string, eticks))
    axs[0][0].plot(epoch, min_pagefaults)
    axs[0][0].set_ylabel("Minor Pagefaults (All threads)")
    axs[0][0].set_xticks(eticks, eticks_formatted)
    axs[1][0].plot(epoch, maj_pagefaults)
    axs[1][0].set_ylabel("Major Pagefaults (All threads)")
    axs[1][0].set_xticks(eticks, eticks_formatted)
    # Show[0] in MiB
    axs[2][0].plot(epoch, np.array(virtual_mem) / 1024 / 1024)
    axs[2][0].set_ylabel("Virtual Memory [MiB]")
    axs[2][0].set_xticks(eticks, eticks_formatted)
    axs[3][0].plot(epoch, np.array(resident_mem))
    axs[3][0].set_ylabel("Resident Memory Pages [#]")
    axs[3][0].set_xticks(eticks, eticks_formatted)

    axs[0][1].plot(epoch, utime, label="utime")
    axs[0][1].plot(epoch, stime, label="stime")
    axs[0][1].set_ylabel("time [s] (All threads)")
    axs[0][1].set_xticks(eticks, eticks_formatted)

    fig.legend()

    fig.savefig(f"{path}/proc.svg")
