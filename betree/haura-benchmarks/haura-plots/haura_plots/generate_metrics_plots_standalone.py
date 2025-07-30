#!/usr/bin/env python3
"""
Generate metrics plots for all benchmark runs - Standalone version
Includes necessary functions from metrics_plots.py and util.py with Python 3.8 compatibility
"""

import os
import sys
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import argparse

try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError as e:
    print(f"ERROR: Error importing required modules: {e}")
    print("Please install required packages:")
    print("  pip install matplotlib numpy")
    sys.exit(1)

# Utility functions from util.py (Python 3.8 compatible)
BLOCK_SIZE = 4096
SEC_MS = 1000
EPOCH_MS = 1000

# Colors
GREEN = '#2ca02c'
BLUE = '#1f77b4'

def subtract_first_index(data):
    """Subtract the first element from all elements in the list."""
    if len(data) > 0:
        first = data[0]
        for i in range(len(data)):
            data[i] -= first

def subtract_last_index(data):
    """Subtract each element from the next element in the list."""
    if len(data) > 1:
        for i in range(len(data) - 1, 0, -1):
            data[i] -= data[i - 1]

def ms_to_string(ms):
    """Convert milliseconds to a formatted string."""
    seconds = ms // 1000
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}:{seconds:02d}"

def num_to_name(tier):
    """Convert a number to the corresponding tier name in the storage hierarchy."""
    names = {0: 'Fastest', 1: 'Fast', 2: 'Slow', 3: 'Slowest'}
    return names.get(tier, f'Tier{tier}')

def read_jsonl(file_handle):
    """Read JSONL file and return list of parsed JSON objects."""
    data = []
    for line in file_handle:
        line = line.strip()
        if line:
            data.append(json.loads(line))
    return data

# Plotting functions from metrics_plots.py (adapted)
def plot_throughput(data, path):
    """Print a four row throughput plot with focussed read or write throughput."""
    
    epoch = [temp['epoch_ms'] for temp in data]
    subtract_first_index(epoch)
    epoch_formatted = list(map(ms_to_string, epoch))
    num_tiers = len(data[0]['storage']['tiers'])
    fig, axs = plt.subplots(num_tiers, 1, figsize=(16,8))
    
    # Handle single tier case
    if num_tiers == 1:
        axs = [axs]
    
    for tier_id in range(num_tiers):
        for disk_id in range(len(data[0]['storage']['tiers'][tier_id]['vdevs'])):
            writes = np.array([])
            reads = np.array([])
            for point in data:
                writes = np.append(writes, point['storage']['tiers'][tier_id]['vdevs'][disk_id]['written'])
                reads = np.append(reads, point['storage']['tiers'][tier_id]['vdevs'][disk_id]['read'])

            if len(writes) > 0:
                subtract_last_index(writes)
                subtract_last_index(reads)

            # convert to MiB from Blocks
            writes = writes * BLOCK_SIZE / 1024 / 1024 * (SEC_MS / EPOCH_MS)
            reads = reads * BLOCK_SIZE / 1024 / 1024 * (SEC_MS / EPOCH_MS)

        axs[tier_id].plot(epoch, reads, label = 'Read', linestyle='dotted', color=GREEN)
        axs[tier_id].plot(epoch, writes, label = 'Written', color=BLUE)
        axs[tier_id].set_xlabel("runtime (minute:seconds)")
        axs[tier_id].set_xticks(epoch, epoch_formatted)
        axs[tier_id].locator_params(tight=True, nbins=10)
        axs[tier_id].set_ylabel(f"{num_to_name(tier_id)}\nMiB/s (I/0)")
        label=' | '.join(path.split('/')[-2:])
    
    fig.legend(loc="center right",handles=axs[0].get_lines())
    fig.suptitle(f"Haura - {label}", y=0.98)
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
    """Plot the utilized space of each storage tier."""
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
        axs[tier].plot((np.array(total[tier]) - np.array(fr)) * 4096 / 1024 / 1024 / 1024, 
                      label="Used", marker="o", markevery=200, color=BLUE)
        axs[tier].plot(np.array(total[tier]) * 4096 / 1024 / 1024 / 1024, 
                      label="Total", marker="^", markevery=200, color=GREEN)
        axs[tier].set_ylim(bottom=0)
        axs[tier].set_ylabel(f"{num_to_name(tier)}\nCapacity in GiB")
        tier += 1

    fig.legend(loc='center right',handles=axs[0].get_lines())
    fig.savefig(f"{path}/tier_usage.svg")
    plt.close(fig)

def plot_system(path):
    """Plot the system usage and temperatures during the run."""
    data = []
    jsonl_file = f"{path}/out.jsonl"
    
    try:
        with open(jsonl_file, 'r', encoding="UTF-8") as metrics:
            data = read_jsonl(metrics)
    except FileNotFoundError:
        print(f"Warning: {jsonl_file} not found, skipping system plot")
        return
    except Exception as e:
        print(f"Warning: Error reading {jsonl_file}: {e}")
        return

    if not data:
        print(f"Warning: No data found in {jsonl_file}")
        return

    epoch = [temp['epoch_ms'] for temp in data]
    subtract_first_index(epoch)
    epoch_formatted = list(map(ms_to_string, epoch))
    min_pagefaults = [x["proc_minflt"] + x["proc_cminflt"] for x in data]
    maj_pagefaults = [x["proc_majflt"] + x["proc_cmajflt"] for x in data]
    virtual_mem = [x["proc_vsize"] for x in data]
    resident_mem = [x["proc_rss"] for x in data]
    utime = [x["proc_utime"] + x["proc_cutime"] for x in data]
    stime = [x["proc_stime"] + x["proc_cstime"] for x in data]

    fig, axs = plt.subplots(3,2, figsize=(10, 10))
    eticks = range(0, epoch[-1:][0], 30 * 10**3)
    eticks_formatted = list(map(ms_to_string, eticks))

    # Page Faults (Minor)
    axs[0][0].plot(epoch, min_pagefaults)
    axs[0][0].set_ylabel("Minor Pagefaults (All threads)")
    axs[0][0].set_xticks(eticks, eticks_formatted)

    # Page Faults (Major)
    axs[1][0].plot(epoch, maj_pagefaults)
    axs[1][0].set_ylabel("Major Pagefaults (All threads)")
    axs[1][0].set_xticks(eticks, eticks_formatted)

    # Show[0] in MiB
    axs[2][0].plot(epoch, np.array(virtual_mem) / 1024 / 1024)
    axs[2][0].set_ylabel("Virtual Memory [MiB]")
    axs[2][0].set_xticks(eticks, eticks_formatted)

    # Resident Memory
    axs[2][1].plot(epoch, np.array(resident_mem))
    axs[2][1].set_ylabel("Resident Memory Pages [#]")
    axs[2][1].set_xticks(eticks, eticks_formatted)

    # CPU time
    axs[0][1].plot(epoch, utime, label="utime")
    axs[0][1].plot(epoch, stime, label="stime")
    axs[0][1].set_ylabel("time [s] (All threads)")
    axs[0][1].set_xticks(eticks, eticks_formatted)
    axs[0][1].legend(bbox_to_anchor=(1.35, 0.6))

    # Temperature plots
    temps_keys = [key for key in data[0].keys() if 'hwmon' in key and 'Tccd' not in key]
    line_styles = ['-', '--', '-.', ':']
    for i, key in enumerate(temps_keys):
        style = line_styles[i % len(line_styles)]
        axs[1][1].plot(epoch, [x[key] for x in data], label=key, linestyle=style)
    
    axs[1][1].set_xticks(eticks, eticks_formatted)
    axs[1][1].set_ylabel("Temperature [C]")
    axs[1][1].legend(bbox_to_anchor=(1.0, 0.6))

    fig.tight_layout()
    fig.savefig(f"{path}/proc.svg")
    plt.close(fig)

class MetricsPlotGenerator:
    """Generate metrics plots for all benchmark runs"""
    
    def __init__(self, results_dirs: List[str], verbose: bool = True):
        self.results_dirs = [Path(d) for d in results_dirs]
        self.verbose = verbose
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'skipped_runs': 0,
            'plots_generated': 0
        }
    
    def log(self, message: str, level: str = "INFO"):
        """Log message if verbose mode is enabled"""
        if self.verbose:
            prefix = {
                "INFO": "INFO: ",
                "SUCCESS": "SUCCESS: ",
                "WARNING": "WARNING: ",
                "ERROR": "ERROR: "
            }.get(level, "")
            print(f"{prefix} {message}")
    
    def find_run_folders(self) -> List[Path]:
        """Find all benchmark run folders in the results directories"""
        run_folders = []
        
        for results_dir in self.results_dirs:
            if not results_dir.exists():
                self.log(f"Results directory not found: {results_dir}", "WARNING")
                continue
                
            self.log(f"Scanning directory: {results_dir}")
            
            for item in results_dir.iterdir():
                if item.is_dir() and self.is_benchmark_run_folder(item):
                    run_folders.append(item)
                    
        run_folders.sort()  # Sort for consistent processing order
        self.log(f"Found {len(run_folders)} benchmark run folders")
        return run_folders
    
    def is_benchmark_run_folder(self, folder: Path) -> bool:
        """Check if a folder is a benchmark run folder"""
        # Check for expected pattern: ycsb_g_entry{size}_{compression}_{timestamp}
        folder_name = folder.name
        if not folder_name.startswith('ycsb_g_entry'):
            return False
            
        # Check for required files
        required_files = ['betree-metrics.jsonl']
        for file_name in required_files:
            if not (folder / file_name).exists():
                return False
                
        return True
    
    def load_metrics_data(self, folder_path: Path) -> Optional[List[Dict[str, Any]]]:
        """Load and parse betree-metrics.jsonl data"""
        metrics_file = folder_path / 'betree-metrics.jsonl'
        
        if not metrics_file.exists():
            self.log(f"Missing betree-metrics.jsonl in {folder_path.name}", "WARNING")
            return None
            
        try:
            data = []
            with open(metrics_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            self.log(f"JSON decode error in {metrics_file} line {line_num}: {e}", "WARNING")
                            continue
            
            if not data:
                self.log(f"No valid data found in {metrics_file}", "WARNING")
                return None
                
            self.log(f"Loaded {len(data)} data points from {folder_path.name}")
            return data
            
        except Exception as e:
            self.log(f"Error loading {metrics_file}: {e}", "ERROR")
            return None
    
    def check_existing_plots(self, folder_path: Path) -> Dict[str, bool]:
        """Check which plots already exist in the folder"""
        plot_files = [
            'plot_write.svg',
            'plot_read.svg', 
            'tier_usage.svg',
            'proc.svg'
        ]
        
        existing = {}
        for plot_file in plot_files:
            existing[plot_file] = (folder_path / plot_file).exists()
            
        return existing
    
    def generate_plots_for_run(self, run_folder: Path, overwrite: bool = False) -> bool:
        """Generate all plots for a single benchmark run"""
        self.log(f"Processing run: {run_folder.name}")
        
        # Check existing plots
        existing_plots = self.check_existing_plots(run_folder)
        if not overwrite and all(existing_plots.values()):
            self.log(f"All plots already exist in {run_folder.name}, skipping", "INFO")
            self.stats['skipped_runs'] += 1
            return True
        
        # Load metrics data
        metrics_data = self.load_metrics_data(run_folder)
        if metrics_data is None:
            self.log(f"Failed to load metrics data for {run_folder.name}", "ERROR")
            self.stats['failed_runs'] += 1
            return False
        
        plots_generated = 0
        
        try:
            # Generate throughput plots (plot_write.svg and plot_read.svg)
            if overwrite or not existing_plots.get('plot_write.svg', False) or not existing_plots.get('plot_read.svg', False):
                self.log(f"Generating throughput plots for {run_folder.name}")
                plot_throughput(metrics_data, str(run_folder))
                plots_generated += 2  # Generates both write and read plots
                
            # Generate tier usage plot (tier_usage.svg)
            if overwrite or not existing_plots.get('tier_usage.svg', False):
                self.log(f"Generating tier usage plot for {run_folder.name}")
                plot_tier_usage(metrics_data, str(run_folder))
                plots_generated += 1
                
            # Generate system plot (proc.svg)
            # Note: plot_system expects out.jsonl, but we have betree-metrics.jsonl
            # Let's check if out.jsonl exists, if not, create a symlink or copy
            out_jsonl = run_folder / 'out.jsonl'
            metrics_jsonl = run_folder / 'betree-metrics.jsonl'
            
            if overwrite or not existing_plots.get('proc.svg', False):
                if not out_jsonl.exists() and metrics_jsonl.exists():
                    # Create symlink from betree-metrics.jsonl to out.jsonl
                    try:
                        out_jsonl.symlink_to('betree-metrics.jsonl')
                        self.log(f"Created symlink out.jsonl -> betree-metrics.jsonl in {run_folder.name}")
                    except Exception as e:
                        self.log(f"Failed to create symlink in {run_folder.name}: {e}", "WARNING")
                        # Try copying instead
                        try:
                            import shutil
                            shutil.copy2(metrics_jsonl, out_jsonl)
                            self.log(f"Copied betree-metrics.jsonl to out.jsonl in {run_folder.name}")
                        except Exception as e2:
                            self.log(f"Failed to copy file in {run_folder.name}: {e2}", "ERROR")
                
                if out_jsonl.exists():
                    self.log(f"Generating system plot for {run_folder.name}")
                    plot_system(str(run_folder))
                    plots_generated += 1
                else:
                    self.log(f"Cannot generate system plot for {run_folder.name}: out.jsonl not available", "WARNING")
            
            self.stats['plots_generated'] += plots_generated
            self.stats['successful_runs'] += 1
            self.log(f"Successfully generated {plots_generated} plots for {run_folder.name}", "SUCCESS")
            return True
            
        except Exception as e:
            self.log(f"Error generating plots for {run_folder.name}: {e}", "ERROR")
            self.stats['failed_runs'] += 1
            return False
    
    def generate_all_plots(self, overwrite: bool = False) -> None:
        """Generate plots for all benchmark runs"""
        self.log("Starting metrics plot generation...")
        
        # Find all run folders
        run_folders = self.find_run_folders()
        if not run_folders:
            self.log("No benchmark run folders found!", "WARNING")
            return
        
        self.stats['total_runs'] = len(run_folders)
        
        # Process each run folder
        for i, run_folder in enumerate(run_folders, 1):
            self.log(f"[{i}/{len(run_folders)}] Processing {run_folder.name}")
            self.generate_plots_for_run(run_folder, overwrite)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print generation summary"""
        print("\n" + "="*60)
        print("METRICS PLOT GENERATION SUMMARY")
        print("="*60)
        print(f"Total runs found:     {self.stats['total_runs']}")
        print(f"Successfully processed: {self.stats['successful_runs']}")
        print(f"Failed:               {self.stats['failed_runs']}")
        print(f"Skipped (existing):   {self.stats['skipped_runs']}")
        print(f"Total plots generated: {self.stats['plots_generated']}")
        
        if self.stats['successful_runs'] > 0:
            success_rate = (self.stats['successful_runs'] / self.stats['total_runs']) * 100
            print(f"Success rate:         {success_rate:.1f}%")
        
        print("="*60)
        
        if self.stats['plots_generated'] > 0:
            print("Plot generation completed!")
            print("Check individual run folders for generated SVG files:")
            print("   - plot_write.svg (write throughput)")
            print("   - plot_read.svg (read throughput)")  
            print("   - tier_usage.svg (storage tier usage)")
            print("   - proc.svg (system metrics)")
        else:
            print("WARNING: No plots were generated.")

def main():
    parser = argparse.ArgumentParser(
        description='Generate metrics plots for all benchmark runs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Use default results directories
  %(prog)s --results-dir ./results/my_run    # Specify custom results directory
  %(prog)s --overwrite                       # Overwrite existing plots
  %(prog)s --quiet                           # Suppress verbose output
        """
    )
    
    parser.add_argument('--results-dir', action='append', dest='results_dirs',
                       help='Results directory to process (can be specified multiple times)')
    
    parser.add_argument('--overwrite', action='store_true',
                       help='Overwrite existing plots')
    
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress verbose output')
    
    args = parser.parse_args()
    
    # Default results directories if none specified
    if not args.results_dirs:
        default_dirs = [
            '/home/skarim/Code/smash/haura/betree/haura-benchmarks/results/2025-07-24_default',
            '/home/skarim/Code/smash/haura/betree/haura-benchmarks/results/2025-07-25_default'
        ]
        args.results_dirs = [d for d in default_dirs if Path(d).exists()]
        
        if not args.results_dirs:
            print("ERROR: No default results directories found!")
            print("Please specify results directories with --results-dir")
            return 1
    
    # Create generator and run
    generator = MetricsPlotGenerator(args.results_dirs, verbose=not args.quiet)
    generator.generate_all_plots(overwrite=args.overwrite)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())