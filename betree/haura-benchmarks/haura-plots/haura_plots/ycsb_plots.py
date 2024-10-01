import os
import pandas as pd
import matplotlib.pyplot as plt

def plot_c(path):
    """
    Bar chart for YCSB-C-esque scalability of a singular run.
    """
    if not os.path.exists(f"{path}/ycsb_c.csv"):
        return

    data = pd.read_csv(f"{path}/ycsb_c.csv");
    fig, ax = plt.subplots()
    # op / s
    first = data["ops"][0] / (data["time_ns"][0] / 10**9)
    second = data["ops"][1] / (data["time_ns"][1] / 10**9)
    # in some cases in local tests the proper scaling behavior only happened
    # with 2 or more threads, this is uncommon but can be easily checked like
    # this to not make the optimal scaling curve entirely useless
    if first < second / 2:
        first = second / 2
    optimal_scaling = [x * first for x in data["threads"]]
    ax.plot(data["threads"], optimal_scaling, linestyle=":", label="Optimal", color='grey')
    ax.bar(data["threads"], data["ops"] / (data["time_ns"] / 10**9))
    ax.set_ylabel("Throughput [op/s]")
    ax.set_title(f"YCSB-C Scaling | {' | '.join(path.split('/')[-2:])}")
    ax.set_xlabel("Threads [#]")
    ax.legend(loc='upper left')
    fig.savefig(f"{path}/ycsb_c.svg")
    return {
        "title": path.split('/')[-1:][0],
        "group": '/'.join(path.split('/')[:-1]),
        "threads": data["threads"],
        "results": data["ops"] / (data["time_ns"] / 10**9),
    }

def plot_grouped_c(path, runs, overall=False):
    """
    Bar chart for YCSB-C-esque scalability over multiple runs.
    """
    if not os.path.exists(path):
        return

    fig, ax = plt.subplots()
    runs = sorted(runs, key=lambda run: max(run["results"]))
    off = 1 / (len(runs) + 1)
    for idx, run in enumerate(runs):
        if not overall:
            title = run["title"]
        else:
            group = run["group"].split('/')[-1:][0]
            title = run["title"] + "[" + group + "]"
        ax.bar(
            [l - off * ((len(runs)-1)/2) + idx * off for l in run["threads"]],
            run["results"],
            off,
            label=title
        )

    if not overall:
        group = runs[0]["group"].split('/')[-1:][0]
        ax.set_title(f'YCSB Scaling | {group}')
    else:
        ax.set_title(f'YCSB-C-esque Write Scaling (Key-Value)')
    ax.set_ylabel("Throughput [op/s]")
    ax.set_xlabel("Threads [#]")
    extra = fig.legend(loc="upper left", bbox_to_anchor=(0.9, 0.89))
    fig.savefig(f"{path}/ycsb_c_comparison.svg", bbox_extra_artists=(extra,), bbox_inches="tight", transparent=True)
