"""
Plots visualizing cache statistics.
"""
from . import util
import numpy as np
import matplotlib.pyplot as plt

def plot_cache(data, path):
    """Plot cache statistics."""
    epoch = [temp['epoch_ms'] for temp in data]
    util.subtract_first_index(epoch)

    fig, axs = plt.subplots(3,1, figsize=(10, 9))
    eticks = range(0, epoch[-1:][0], 30 * 10**3);
    eticks_formatted = list(map(util.ms_to_string, eticks))

    # Capacity vs Size (Peak Check)
    cap = np.array([temp['cache']['capacity'] / 1024 / 1024 for temp in data])
    axs[0].plot(epoch, cap, label='capacity')
    axs[0].plot(epoch, [temp['cache']['size'] / 1024 / 1024 for temp in data], label='size')
    axs[0].set_xticks(eticks, eticks_formatted)
    axs[0].set_ylabel("Size [MiB]")
    oax = axs[0].twinx()
    elems = np.array([temp['cache']['len'] for temp in data])
    oax.plot(epoch, elems, label='entries', drawstyle='steps')
    oax.set_ylim(top=elems.max() * 1.4)
    oax.legend(bbox_to_anchor=(1.0, 1.2))
    oax.set_ylabel("# of entries")
    axs[0].legend(ncols=2, bbox_to_anchor=(0.8, 1.2))

    # Hits vs Misses (Keep one high, the other low)
    hits = np.array(util.diff_window([temp['cache']['hits'] for temp in data]))
    miss = np.array(util.diff_window([temp['cache']['misses'] for temp in data]))
    axs[1].plot(epoch, hits, label='hits')
    axs[1].plot(epoch, miss, label='misses')
    axs[1].set_xticks(eticks, eticks_formatted)
    axs[1].set_ylabel("# per 500ms")
    oax = axs[1].twinx()
    np.seterr(divide='ignore')
    oax.plot(epoch, hits / (hits + miss) * 100, label="Hit-Miss-Ratio", linestyle=':')
    oax.set_ylabel("Hits [%]")
    oax.legend(bbox_to_anchor=(1.0, 1.2))
    axs[1].legend(ncols=2, bbox_to_anchor=(0.8, 1.2))

    # insertions (reads, new nodes, updates, writes) vs evictions (updates) vs removals (reads)
    axs[2].plot(epoch, util.diff_window([temp['cache']['insertions'] for temp in data]), label='insertions')
    axs[2].plot(epoch, util.diff_window([temp['cache']['evictions'] for temp in data]), label='evictions')
    axs[2].plot(epoch, util.diff_window([temp['cache']['removals'] for temp in data]), label='removals')
    axs[2].set_xticks(eticks, eticks_formatted)
    axs[2].set_ylabel("# per 500ms")
    axs[2].legend(ncols=3, bbox_to_anchor=(1.0, 1.2))
    axs[2].set_xlabel("Time [m:s]")

    label=' | '.join(path.split('/')[-2:])
    fig.suptitle(f"Haura - {label}")
    fig.tight_layout()
    fig.savefig(
        f"{path}/cache_stats.svg",
    )
    plt.close(fig)
