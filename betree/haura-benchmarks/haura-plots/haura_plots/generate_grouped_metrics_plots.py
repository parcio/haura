#!/usr/bin/env python3.12
"""
Generate grouped metrics plots by chunk size for all benchmark runs.
This script groups benchmark runs by chunk size and creates combined plots
showing different compression algorithms and thread counts on the same plot.
"""

import os
import sys
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import argparse
import numpy as np
import matplotlib.pyplot as plt
from itertools import cycle

# Add the haura_plots directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from haura_plots import util
    print("Successfully imported haura_plots utilities!")
except ImportError as e:
    print(f"ERROR: Error importing haura_plots modules: {e}")
    print("Make sure you're running this script with Python 3.12+ from the haura-plots directory")
    sys.exit(1)

class GroupedMetricsPlotGenerator:
    """Generate grouped metrics plots by chunk size"""
    
    def __init__(self, results_dirs: List[str], verbose: bool = True):
        self.results_dirs = [Path(d) for d in results_dirs]
        self.verbose = verbose
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'chunk_groups': 0,
            'plots_generated': 0
        }
        
        # Define colors and markers for different combinations
        self.colors = ['#0072B2', '#E69F00', '#009E73', '#F0E442', '#D55E00', '#CC79A7', '#56B4E9', '#000000']
        self.markers = ['o', 's', '^', 'v', '<', '>', 'D', 'p', '*', 'h', 'H', '+', 'x']
        self.linestyles = ['-', '--', '-.', ':']
        
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
    
    def extract_chunk_size_from_folder_name(self, folder_name: str) -> Optional[int]:
        """Extract chunk size from folder name like ycsb_g_entry4096_zstd10_1753388818"""
        match = re.match(r'ycsb_g_entry(\d+)_.*', folder_name)
        if match:
            return int(match.group(1))
        return None
    
    def extract_compression_from_config(self, config_path: Path) -> str:
        """Extract compression algorithm from config file"""
        try:
            with open(config_path, 'r') as f:
                content = f.read()
                
            # Parse compression from config
            if 'compression: None' in content:
                return 'None'
            elif 'compression: Zstd(' in content:
                # Extract level
                level_match = re.search(r'level: (\d+)', content)
                if level_match:
                    return f'Zstd({level_match.group(1)})'
                return 'Zstd'
            elif 'compression: Lz4(' in content:
                # Extract level
                level_match = re.search(r'level: (\d+)', content)
                if level_match:
                    return f'Lz4({level_match.group(1)})'
                return 'Lz4'
            elif 'compression: Snappy' in content:
                return 'Snappy'
            else:
                return 'Unknown'
                
        except Exception as e:
            self.log(f"Error reading config file {config_path}: {e}", "WARNING")
            return 'Unknown'
    
    def extract_thread_count_from_csv(self, csv_path: Path) -> Optional[int]:
        """Extract thread count from ycsb_g.csv file"""
        try:
            with open(csv_path, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    # Second line, first value
                    return int(lines[1].split(',')[0])
        except Exception as e:
            self.log(f"Error reading CSV file {csv_path}: {e}", "WARNING")
        return None
    
    def load_metrics_data(self, folder_path: Path) -> Optional[List[Dict[str, Any]]]:
        """Load and parse betree-metrics.jsonl data"""
        metrics_file = folder_path / 'betree-metrics.jsonl'
        
        if not metrics_file.exists():
            self.log(f"Missing betree-metrics.jsonl in {folder_path.name}", "WARNING")
            return None
            
        try:
            with open(metrics_file, 'r', encoding="UTF-8") as f:
                data = util.read_jsonl(f)
            
            if not data:
                self.log(f"No valid data found in {metrics_file}", "WARNING")
                return None
                
            return data
            
        except Exception as e:
            self.log(f"Error loading {metrics_file}: {e}", "ERROR")
            return None
    
    def find_and_group_runs(self) -> Dict[int, List[Dict[str, Any]]]:
        """Find all benchmark runs and group them by chunk size"""
        grouped_runs = {}
        
        for results_dir in self.results_dirs:
            if not results_dir.exists():
                self.log(f"Results directory not found: {results_dir}", "WARNING")
                continue
                
            self.log(f"Scanning directory: {results_dir}")
            
            for item in results_dir.iterdir():
                if not item.is_dir() or not item.name.startswith('ycsb_g_entry'):
                    continue
                
                # Extract chunk size
                chunk_size = self.extract_chunk_size_from_folder_name(item.name)
                if chunk_size is None:
                    continue
                
                # Check required files
                if not (item / 'betree-metrics.jsonl').exists():
                    continue
                
                # Extract compression
                compression = self.extract_compression_from_config(item / 'config')
                
                # Extract thread count
                thread_count = self.extract_thread_count_from_csv(item / 'ycsb_g.csv')
                if thread_count is None:
                    continue
                
                # Load metrics data
                metrics_data = self.load_metrics_data(item)
                if metrics_data is None:
                    self.stats['failed_runs'] += 1
                    continue
                
                # Group by chunk size
                if chunk_size not in grouped_runs:
                    grouped_runs[chunk_size] = []
                
                grouped_runs[chunk_size].append({
                    'folder_path': item,
                    'folder_name': item.name,
                    'chunk_size': chunk_size,
                    'compression': compression,
                    'thread_count': thread_count,
                    'metrics_data': metrics_data
                })
                
                self.stats['successful_runs'] += 1
        
        self.stats['total_runs'] = self.stats['successful_runs'] + self.stats['failed_runs']
        self.stats['chunk_groups'] = len(grouped_runs)
        
        # Sort runs within each group for consistent ordering
        for chunk_size in grouped_runs:
            grouped_runs[chunk_size].sort(key=lambda x: (x['compression'], x['thread_count']))
        
        return grouped_runs
    
    def get_style_for_run(self, run_info: Dict[str, Any], style_index: int) -> Tuple[str, str, str]:
        """Get color, marker, and linestyle for a run"""
        color = self.colors[style_index % len(self.colors)]
        marker = self.markers[style_index % len(self.markers)]
        linestyle = self.linestyles[style_index % len(self.linestyles)]
        return color, marker, linestyle
    
    def create_run_label(self, run_info: Dict[str, Any]) -> str:
        """Create a descriptive label for a run"""
        return f"{run_info['compression']}_T{run_info['thread_count']}"
    
    def plot_grouped_throughput(self, chunk_size: int, runs: List[Dict[str, Any]], output_dir: Path):
        """Create grouped throughput plots for a chunk size"""
        if not runs:
            return
        
        # Determine number of tiers from first run
        first_data = runs[0]['metrics_data']
        num_tiers = len(first_data[0]['storage']['tiers'])
        
        # Create plots for write and read
        for plot_type in ['write', 'read']:
            fig, axs = plt.subplots(num_tiers, 1, figsize=(16, 8))
            if num_tiers == 1:
                axs = [axs]  # Make it iterable
            
            # Process each run
            for run_idx, run_info in enumerate(runs):
                data = run_info['metrics_data']
                color, marker, linestyle = self.get_style_for_run(run_info, run_idx)
                label = self.create_run_label(run_info)
                
                # Prepare epoch data
                epoch = [temp['epoch_ms'] for temp in data]
                util.subtract_first_index(epoch)
                epoch_formatted = list(map(util.ms_to_string, epoch))
                
                # Plot each tier
                for tier_id in range(num_tiers):
                    tier_writes = np.array([])
                    tier_reads = np.array([])
                    
                    # Check if this tier exists in the data
                    if tier_id >= len(data[0]['storage']['tiers']):
                        continue
                    
                    # Aggregate data across all disks in this tier
                    num_disks = len(data[0]['storage']['tiers'][tier_id]['vdevs'])
                    for disk_id in range(num_disks):
                        writes = np.array([])
                        reads = np.array([])
                        
                        for point in data:
                            if (tier_id < len(point['storage']['tiers']) and 
                                disk_id < len(point['storage']['tiers'][tier_id]['vdevs'])):
                                writes = np.append(writes, point['storage']['tiers'][tier_id]['vdevs'][disk_id]['written'])
                                reads = np.append(reads, point['storage']['tiers'][tier_id]['vdevs'][disk_id]['read'])
                        
                        if len(writes) > 0:
                            util.subtract_last_index(writes)
                            util.subtract_last_index(reads)
                        
                        # Convert to MiB/s
                        writes = writes * util.BLOCK_SIZE / 1024 / 1024 * (util.SEC_MS / util.EPOCH_MS)
                        reads = reads * util.BLOCK_SIZE / 1024 / 1024 * (util.SEC_MS / util.EPOCH_MS)
                        
                        if len(tier_writes) == 0:
                            tier_writes = writes.copy()
                            tier_reads = reads.copy()
                        else:
                            tier_writes += writes
                            tier_reads += reads
                    
                    # Plot the appropriate data (only if we have data)
                    if len(tier_writes) > 0 and len(tier_reads) > 0:
                        if plot_type == 'write':
                            axs[tier_id].plot(epoch, tier_writes, label=label, color=color, 
                                            marker=marker, linestyle=linestyle, markevery=max(1, len(epoch)//20))
                        else:
                            axs[tier_id].plot(epoch, tier_reads, label=label, color=color, 
                                            marker=marker, linestyle=linestyle, markevery=max(1, len(epoch)//20))
                    else:
                        self.log(f"No data for {label} tier {tier_id}", "WARNING")
                    
                    axs[tier_id].set_xlabel("runtime (minute:seconds)")
                    axs[tier_id].set_xticks(epoch[::max(1, len(epoch)//10)], 
                                          [epoch_formatted[i] for i in range(0, len(epoch), max(1, len(epoch)//10))])
                    axs[tier_id].set_ylabel(f"{util.num_to_name(tier_id)}\nMiB/s (I/O)")
            
            # Add legend and title
            fig.legend(loc="center right", bbox_to_anchor=(1.15, 0.5))
            fig.suptitle(f"Haura - Chunk Size {chunk_size} - {plot_type.title()} Throughput", y=0.98)
            
            # Save plot
            output_file = output_dir / f"chunk_{chunk_size}_{plot_type}_throughput.svg"
            fig.tight_layout()
            fig.savefig(output_file, bbox_inches='tight')
            plt.close(fig)
            
            self.log(f"Generated {plot_type} throughput plot for chunk size {chunk_size}")
            self.stats['plots_generated'] += 1
    
    def plot_grouped_tier_usage(self, chunk_size: int, runs: List[Dict[str, Any]], output_dir: Path):
        """Create grouped tier usage plot for a chunk size"""
        if not runs:
            return
        
        fig, axs = plt.subplots(4, 1, figsize=(12, 13))
        
        # Process each run
        for run_idx, run_info in enumerate(runs):
            data = run_info['metrics_data']
            color, marker, linestyle = self.get_style_for_run(run_info, run_idx)
            label = self.create_run_label(run_info)
            
            # Extract tier usage data
            free = [[], [], [], []]
            total = [[], [], [], []]
            
            for ts in data:
                if "usage" in ts:
                    tier = 0
                    for stat in ts["usage"]:
                        if tier < 4:  # Ensure we don't exceed our arrays
                            free[tier].append(stat["free"])
                            total[tier].append(stat["total"])
                            tier += 1
            
            # Plot each tier
            for tier in range(4):
                if tier < len(free) and len(free[tier]) > 0:
                    used_data = (np.array(total[tier]) - np.array(free[tier])) * 4096 / 1024 / 1024 / 1024
                    total_data = np.array(total[tier]) * 4096 / 1024 / 1024 / 1024
                    
                    # Plot used space
                    axs[tier].plot(used_data, label=f"{label} (Used)", color=color, 
                                 marker=marker, linestyle=linestyle, markevery=max(1, len(used_data)//20))
                    
                    axs[tier].set_ylim(bottom=0)
                    axs[tier].set_ylabel(f"{util.num_to_name(tier)}\nCapacity in GiB")
        
        # Add legend and title
        fig.legend(loc='center right', bbox_to_anchor=(1.15, 0.5))
        fig.suptitle(f"Haura - Chunk Size {chunk_size} - Tier Usage", y=0.98)
        
        # Save plot
        output_file = output_dir / f"chunk_{chunk_size}_tier_usage.svg"
        fig.tight_layout()
        fig.savefig(output_file, bbox_inches='tight')
        plt.close(fig)
        
        self.log(f"Generated tier usage plot for chunk size {chunk_size}")
        self.stats['plots_generated'] += 1
    
    def plot_grouped_system(self, chunk_size: int, runs: List[Dict[str, Any]], output_dir: Path):
        """Create grouped system metrics plot for a chunk size"""
        if not runs:
            return
        
        fig, axs = plt.subplots(3, 2, figsize=(14, 12))
        
        # Process each run
        for run_idx, run_info in enumerate(runs):
            # Load system metrics from out.jsonl file
            out_jsonl_path = run_info['folder_path'] / 'out.jsonl'
            if not out_jsonl_path.exists():
                self.log(f"No out.jsonl file found for {run_info['folder_name']}", "WARNING")
                continue
                
            try:
                with open(out_jsonl_path, 'r', encoding="UTF-8") as f:
                    system_data = util.read_jsonl(f)
            except Exception as e:
                self.log(f"Error loading system data for {run_info['folder_name']}: {e}", "WARNING")
                continue
            
            if not system_data:
                continue
                
            color, marker, linestyle = self.get_style_for_run(run_info, run_idx)
            label = self.create_run_label(run_info)
            
            # Extract system metrics
            epoch = [temp['epoch_ms'] for temp in system_data]
            util.subtract_first_index(epoch)
            epoch_formatted = list(map(util.ms_to_string, epoch))
            
            min_pagefaults = [x["proc_minflt"] + x["proc_cminflt"] for x in system_data]
            maj_pagefaults = [x["proc_majflt"] + x["proc_cmajflt"] for x in system_data]
            virtual_mem = [x["proc_vsize"] for x in system_data]
            resident_mem = [x["proc_rss"] for x in system_data]
            utime = [x["proc_utime"] + x["proc_cutime"] for x in system_data]
            stime = [x["proc_stime"] + x["proc_cstime"] for x in system_data]
            
            # Plot system metrics
            markevery = max(1, len(epoch)//20)
            
            # Minor page faults
            axs[0][0].plot(epoch, min_pagefaults, label=label, color=color, 
                          marker=marker, linestyle=linestyle, markevery=markevery)
            axs[0][0].set_ylabel("Minor Pagefaults")
            
            # Major page faults
            axs[1][0].plot(epoch, maj_pagefaults, label=label, color=color, 
                          marker=marker, linestyle=linestyle, markevery=markevery)
            axs[1][0].set_ylabel("Major Pagefaults")
            
            # Virtual memory
            axs[2][0].plot(epoch, np.array(virtual_mem) / 1024 / 1024, label=label, color=color, 
                          marker=marker, linestyle=linestyle, markevery=markevery)
            axs[2][0].set_ylabel("Virtual Memory [MiB]")
            
            # Resident memory
            axs[2][1].plot(epoch, np.array(resident_mem), label=label, color=color, 
                          marker=marker, linestyle=linestyle, markevery=markevery)
            axs[2][1].set_ylabel("Resident Memory Pages")
            
            # CPU time (utime)
            axs[0][1].plot(epoch, utime, label=f"{label} (utime)", color=color, 
                          marker=marker, linestyle='-', markevery=markevery)
            axs[0][1].plot(epoch, stime, label=f"{label} (stime)", color=color, 
                          marker=marker, linestyle='--', markevery=markevery)
            axs[0][1].set_ylabel("CPU Time [s]")
            
            # Temperature (if available)
            temps_keys = [key for key in system_data[0].keys() if 'hwmon' in key and 'Tccd' not in key]
            if temps_keys:
                for key in temps_keys[:1]:  # Just plot first temperature sensor to avoid clutter
                    axs[1][1].plot(epoch, [x[key] for x in system_data], label=f"{label} ({key})", 
                                  color=color, marker=marker, linestyle=linestyle, markevery=markevery)
                axs[1][1].set_ylabel("Temperature [C]")
        
        # Format x-axis for all subplots (use a reasonable default if no data)
        if runs:
            # Get epoch from first successful run for x-axis formatting
            sample_epoch = None
            for run_info in runs:
                out_jsonl_path = run_info['folder_path'] / 'out.jsonl'
                if out_jsonl_path.exists():
                    try:
                        with open(out_jsonl_path, 'r', encoding="UTF-8") as f:
                            sample_data = util.read_jsonl(f)
                        if sample_data:
                            sample_epoch = [temp['epoch_ms'] for temp in sample_data]
                            util.subtract_first_index(sample_epoch)
                            break
                    except:
                        continue
            
            if sample_epoch:
                eticks = range(0, sample_epoch[-1], 30 * 10**3)
                eticks_formatted = list(map(util.ms_to_string, eticks))
            else:
                eticks = []
                eticks_formatted = []
        else:
            eticks = []
            eticks_formatted = []
        
        for i in range(3):
            for j in range(2):
                if eticks:
                    axs[i][j].set_xticks(eticks, eticks_formatted)
                axs[i][j].set_xlabel("runtime (minute:seconds)")
        
        # Add legend and title
        fig.legend(loc='center right', bbox_to_anchor=(1.15, 0.5))
        fig.suptitle(f"Haura - Chunk Size {chunk_size} - System Metrics", y=0.98)
        
        # Save plot
        output_file = output_dir / f"chunk_{chunk_size}_system.svg"
        fig.tight_layout()
        fig.savefig(output_file, bbox_inches='tight')
        plt.close(fig)
        
        self.log(f"Generated system metrics plot for chunk size {chunk_size}")
        self.stats['plots_generated'] += 1
    
    def generate_grouped_plots(self, output_dir: Optional[str] = None) -> None:
        """Generate all grouped plots"""
        self.log("Starting grouped metrics plot generation...")
        
        # Find and group runs
        grouped_runs = self.find_and_group_runs()
        if not grouped_runs:
            self.log("No benchmark runs found!", "WARNING")
            return
        
        # Determine output directory
        if output_dir:
            output_path = Path(output_dir)
        else:
            # If processing multiple directories, use parent directory
            # If processing single directory, use that directory
            if len(self.results_dirs) > 1:
                output_path = self.results_dirs[0]
            else:
                output_path = self.results_dirs[0]
        
        output_path.mkdir(exist_ok=True)
        print(f"Saving grouped plots to: {output_path}")
        
        # Generate plots for each chunk size group
        for chunk_size in sorted(grouped_runs.keys()):
            runs = grouped_runs[chunk_size]
            self.log(f"Generating plots for chunk size {chunk_size} ({len(runs)} runs)")
            
            # Generate throughput plots
            self.plot_grouped_throughput(chunk_size, runs, output_path)
            
            # Generate tier usage plot
            self.plot_grouped_tier_usage(chunk_size, runs, output_path)
            
            # Generate system metrics plot
            self.plot_grouped_system(chunk_size, runs, output_path)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print generation summary"""
        print("\n" + "="*60)
        print("GROUPED METRICS PLOT GENERATION SUMMARY")
        print("="*60)
        print(f"Total runs found:       {self.stats['total_runs']}")
        print(f"Successfully processed: {self.stats['successful_runs']}")
        print(f"Failed:                 {self.stats['failed_runs']}")
        print(f"Chunk size groups:      {self.stats['chunk_groups']}")
        print(f"Total plots generated:  {self.stats['plots_generated']}")
        
        if self.stats['successful_runs'] > 0:
            success_rate = (self.stats['successful_runs'] / self.stats['total_runs']) * 100
            print(f"Success rate:           {success_rate:.1f}%")
        
        print("="*60)
        
        if self.stats['plots_generated'] > 0:
            print("Grouped plot generation completed!")
            print("Generated plot types per chunk size:")
            print("   - chunk_XXXX_write_throughput.svg")
            print("   - chunk_XXXX_read_throughput.svg")
            print("   - chunk_XXXX_tier_usage.svg")
            print("   - chunk_XXXX_system.svg")
            print("\nPlots show combined data grouped by chunk size!")
        else:
            print("WARNING: No plots were generated.")

def main():
    parser = argparse.ArgumentParser(
        description='Generate grouped metrics plots by chunk size',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Use default results directories
  %(prog)s --results-dir ./results/my_run    # Specify custom results directory
  %(prog)s --output-dir ./grouped_plots      # Specify output directory
  %(prog)s --quiet                           # Suppress verbose output

This script groups benchmark runs by chunk size and creates combined plots
showing different compression algorithms and thread counts on the same plot.
        """
    )
    
    parser.add_argument('--results-dir', action='append', dest='results_dirs',
                       help='Results directory to process (can be specified multiple times)')
    
    parser.add_argument('--output-dir', 
                       help='Output directory for grouped plots (default: parent dir if multiple results dirs, otherwise the results dir itself)')
    
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
    generator = GroupedMetricsPlotGenerator(args.results_dirs, verbose=not args.quiet)
    generator.generate_grouped_plots(args.output_dir)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())