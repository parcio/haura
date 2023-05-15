#!/bin/env python3
"""
This script produces a number of plots to be turned into a video for clarity
we only portray a single tier, the result is mainly intended as a learning and
debugging aid.
The amount of data analyzed and created by this script is potentially huge so
be aware of disk usage.
"""

from glob import glob
import json
from multiprocessing import Pool
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from tqdm import tqdm
import numpy
import math
import subprocess

FRAGMENTATION_TXT = "alloc_tries.txt"

def fragmentation_plot(fig, end=2**128):
    """
    Insert into an existing figure the fragmentation plot until a (maybe) specified end.
    2**128 is the maximum length which is possible due to name restrictions of the logs.
    """
    file = open(FRAGMENTATION_TXT)
    vals = [int("0" + line) for line in file.readlines()][:end]
    ax = fig.subplots(1,1)
    ax.plot(range(0, len(vals)), vals)
    ax.set_title("Fragmentation over time\nHigher # of allocation retries relates to higher fragmentation")
    ax.set_xlabel("Allocation #")
    ax.set_ylabel("# of allocation retries")

def file_to_plot(path):
    """
    Plot a single frame of the allocation bitmaps with adjacent data of space
    accounting and fragmentation indication.
    """
    name = path[:-5]
    allocs = json.load(open(path))
    pixel_per_bar = 2
    dpi = 100
    label_width = 200
    inches = len(allocs[0][0][0])*(pixel_per_bar * 512 / dpi)
    fig = plt.figure(figsize=(inches + (label_width / dpi), inches), dpi=dpi)
    disks = len(allocs[0])
    subfigs = fig.subfigures(disks + 1, len(allocs[0][0][0]) + 1, squeeze=False)
    for disk in range(0, disks):
        for idx, segment in enumerate(allocs[0][disk][0]):
            vec_expand = numpy.vectorize(u8_to_8_x_u8, otypes=[numpy.ndarray])
            np_seg = vec_expand(numpy.array(segment["data"]))
            np_seg = numpy.concatenate([x.ravel() for x in np_seg])
            sub = subfigs[disk][idx].add_axes([(label_width / dpi)/inches,0,1,0.95])
            subfigs[disk][idx].text(0,0.5, f"disk {disk}\nseg. {idx}", fontsize="x-large")
            sub.set_axis_off()
            subfigs[disk][idx].add_artist(Rectangle(
                [(label_width / dpi)/inches,0],
                1 - (label_width / dpi)/inches,
                0.95,
                fill=None,
                edgecolor="black",
                linewidth=2
            ))
            sub.imshow(np_seg.reshape(512, 512), cmap='binary', aspect='auto', interpolation='nearest')
        ax = subfigs[disk][-1:][0].subplots(1,1)
        sizes = allocs[0][disk][1]
        ax.plot(range(0, len(sizes)), sizes)
        ax.set_ylim(bottom=0)
        ax.set_title(f"Free pages on disk {disk}")
        ax.set_ylabel("# of free pages")
        ax.set_xlabel("Allocation #")
    fragmentation_plot(subfigs[-1:][0][0], end=int(name))
    fig.savefig(name + ".jpg")
    plt.close(fig)

def alloc_tries_plot():
    """
    Plot the fragmentation indication over time in a separate file.
    """
    fig = plt.figure()
    fragmentation_plot(fig)
    fig.savefig("alloc_tries_plot.svg")

def u8_to_8_x_u8(num):
    return numpy.array([
        (num & (1 << 0)) >> 0,
        (num & (1 << 1)) >> 1,
        (num & (1 << 2)) >> 2,
        (num & (1 << 3)) >> 3,
        (num & (1 << 4)) >> 4,
        (num & (1 << 5)) >> 5,
        (num & (1 << 6)) >> 6,
        (num & (1 << 7)) >> 7,
    ])

alloc_tries_plot()
p = Pool() # use all available CPUs
files = glob("*.json")
for task in tqdm(p.imap(file_to_plot, files), total=len(files)):
    continue
# merge frames to video
subprocess.run("ffmpeg -framerate 20 -pattern_type glob -i '*.jpg' -c:v libx264 out.mp4", shell=True, check=True)
