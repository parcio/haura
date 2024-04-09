import os
import pandas as pd
import matplotlib.pyplot as plt

def plot_c(path):
    if not os.path.exists(f"{path}/ycsb_c.csv"):
        return

    data = pd.read_csv(f"{path}/ycsb_c.csv");
    fig, ax = plt.subplots()
    # op / s
    first = data["ops"][0] / (data["time_ns"][0] / 10**9)
    second = data["ops"][1] / (data["time_ns"][1] / 10**9)
    if first < second / 2:
        first = second / 2
    optimal_scaling = [x * first for x in data["threads"]]
    ax.plot(data["threads"], optimal_scaling, linestyle=":", label="Optimal", color='grey')
    ax.bar(data["threads"], data["ops"] / (data["time_ns"] / 10**9))
    ax.set_ylabel("Throughput [op/s]")
    ax.set_title("YCSB-C Scaling Behavior")
    ax.set_xlabel("Threads [#]")
    fig.legend()
    fig.savefig(f"{path}/ycsb_c.svg")
