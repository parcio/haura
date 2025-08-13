#!/usr/bin/env python3
"""
Comprehensive Heatmap Generator for Haura Benchmark Results

This script generates heatmaps for various performance metrics across different
configurations (entry size, compression type, thread count).
"""

import os
import json
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
from typing import Dict, List, Tuple, Optional

class BenchmarkHeatmapGenerator:
    def __init__(self, results_dir: str, ycsb_char: str, remove_empty: bool = True):
        self.results_dir = Path(results_dir)
        self.ycsb_char = ycsb_char
        self.remove_empty = remove_empty
        self.data = {}
        self.metrics = {}
        
        # Define expected configurations
        self.entry_sizes = [512, 4096, 16384, 32768]  # 512, 4k, 16k, 32k
        self.compression_types = ['None', 'Snappy', 'Rle', 'Delta', 'Zstd(1)', 'Zstd(5)', 'Zstd(10)', 'Lz4(1)', 'Lz4(5)', 'Lz4(10)']
        self.thread_counts = [1, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40]
        
    def parse_folder_name(self, folder_name: str) -> Optional[Tuple[int, str, int]]:
        """Parse folder name to extract entry size, compression, and timestamp"""
        # Pattern: ycsb_{char}_entry{size}_{compression}_{timestamp}
        # Split by underscores and reconstruct
        if not folder_name.startswith(f'ycsb_{self.ycsb_char}_entry'):
            return None
        
        parts = folder_name.split('_')
        if len(parts) < 4:  # ycsb, {char}, entry{size}, compression..., timestamp
            return None
        
        try:
            # Extract entry size from "entry{size}" (handles both "entry512" and "entry4k")
            entry_part = parts[2]  # "entry512", "entry4k", etc.
            if not entry_part.startswith('entry'):
                return None
            
            size_str = entry_part[5:]  # Remove "entry" prefix
            if size_str.endswith('k'):
                # Convert k suffix to actual bytes (e.g., "4k" -> 4096)
                entry_size = int(size_str[:-1]) * 1024
            else:
                entry_size = int(size_str)
            
            # Last part is always timestamp
            timestamp = int(parts[-1])
            
            # Everything between entry size and timestamp is compression
            compression_parts = parts[3:-1]  # Skip ycsb_{char}_entry{size} and timestamp
            compression_raw = '_'.join(compression_parts)
            
            # Map compression names
            compression_map = {
                'none': 'None',
                'snappy': 'Snappy',
                'rle': 'Rle',
                'delta': 'Delta',
                'zstd1': 'Zstd(1)',
                'zstd5': 'Zstd(5)',
                'zstd10': 'Zstd(10)',
                'lz4_1': 'Lz4(1)',
                'lz4_5': 'Lz4(5)',
                'lz4_10': 'Lz4(10)'
            }
            
            compression = compression_map.get(compression_raw, compression_raw)
            return entry_size, compression, timestamp
            
        except (ValueError, IndexError):
            return None
    
    def extract_thread_count(self, folder_path: Path) -> Optional[int]:
        """Extract thread count from ycsb_{char}.csv file"""
        csv_file = folder_path / f'ycsb_{self.ycsb_char}.csv'
        if not csv_file.exists():
            return None
            
        try:
            with open(csv_file, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    # Second line, first value
                    thread_count = int(lines[1].split(',')[0])
                    return thread_count
        except (ValueError, IndexError, IOError):
            pass
        return None
    
    def extract_compression_from_config(self, folder_path: Path) -> Optional[str]:
        """Extract compression type from config file as backup"""
        config_file = folder_path / 'config'
        if not config_file.exists():
            return None
            
        try:
            with open(config_file, 'r') as f:
                content = f.read()
                
                if 'compression: None' in content:
                    return 'None'
                elif 'compression: Rle(' in content:
                    return 'Rle'
                elif 'compression: Delta(' in content:
                    return 'Delta'
                elif 'Zstd' in content:
                    if 'level: 1' in content:
                        return 'Zstd(1)'
                    elif 'level: 5' in content:
                        return 'Zstd(5)'
                    elif 'level: 10' in content:
                        return 'Zstd(10)'
                elif 'Lz4' in content:
                    if 'level: 1' in content:
                        return 'Lz4(1)'
                    elif 'level: 5' in content:
                        return 'Lz4(5)'
                    elif 'level: 10' in content:
                        return 'Lz4(10)'
                elif 'Snappy' in content:
                    return 'Snappy'
        except IOError:
            pass
        return None
    
    def calculate_metrics(self, folder_path: Path) -> Dict:
        """Calculate all metrics for a single benchmark run"""
        betree_file = folder_path / 'betree-metrics.jsonl'
        out_file = folder_path / 'out.jsonl'
        
        if not betree_file.exists() or not out_file.exists():
            return {}
        
        try:
            # Load betree metrics
            betree_data = []
            with open(betree_file, 'r') as f:
                for line in f:
                    betree_data.append(json.loads(line.strip()))
            
            # Load system metrics
            out_data = []
            with open(out_file, 'r') as f:
                for line in f:
                    out_data.append(json.loads(line.strip()))
            
            if not betree_data or not out_data:
                return {}
            
            final_betree = betree_data[-1]
            final_out = out_data[-1]
            runtime_sec = (final_betree['epoch_ms'] - betree_data[0]['epoch_ms']) / 1000
            
            if runtime_sec <= 0:
                return {}
            
            # Extract basic I/O data
            total_written = final_betree['storage']['tiers'][0]['vdevs'][0]['written']
            total_read = final_betree['storage']['tiers'][0]['vdevs'][0]['read']
            
            # Extract memory read data (bytes read from memory)
            total_memory_read = final_betree['storage']['tiers'][0]['vdevs'][0].get('memory_read', 0)
            
            # Extract memory read count (number of memory read operations)
            total_memory_read_count = final_betree['storage']['tiers'][0]['vdevs'][0].get('memory_read_count', 0)
            
            # Extract compression metrics
            bytes_to_compressed = final_betree['storage']['tiers'][0]['vdevs'][0].get('bytes_to_compressed', 0)
            compressed_bytes = final_betree['storage']['tiers'][0]['vdevs'][0].get('compressed_bytes', 0)
            
            # Extract compression and decompression time (in nanoseconds)
            total_compression_time_ns = final_betree['storage']['tiers'][0]['vdevs'][0].get('compression_time', 0)
            total_decompression_time_ns = final_betree['storage']['tiers'][0]['vdevs'][0].get('decompression_time', 0)
            
            # Calculate throughput metrics
            BLOCK_SIZE = 4096
            avg_write_throughput = (total_written * BLOCK_SIZE / 1024 / 1024) / runtime_sec
            # Include both disk reads and memory reads in read throughput
            total_read_bytes = (total_read * BLOCK_SIZE) + total_memory_read
            avg_read_throughput = (total_read_bytes / 1024 / 1024) / runtime_sec
            
            # Calculate peak throughput
            write_incremental = []
            read_incremental = []
            for i in range(1, len(betree_data)):
                prev_written = betree_data[i-1]['storage']['tiers'][0]['vdevs'][0]['written']
                curr_written = betree_data[i]['storage']['tiers'][0]['vdevs'][0]['written']
                prev_read = betree_data[i-1]['storage']['tiers'][0]['vdevs'][0]['read']
                curr_read = betree_data[i]['storage']['tiers'][0]['vdevs'][0]['read']
                
                # Include memory read increments in read throughput
                prev_memory_read = betree_data[i-1]['storage']['tiers'][0]['vdevs'][0].get('memory_read', 0)
                curr_memory_read = betree_data[i]['storage']['tiers'][0]['vdevs'][0].get('memory_read', 0)
                
                write_incremental.append(curr_written - prev_written)
                # Combine disk reads and memory reads for total read throughput
                disk_read_bytes = (curr_read - prev_read) * BLOCK_SIZE
                memory_read_bytes = curr_memory_read - prev_memory_read
                total_read_increment = disk_read_bytes + memory_read_bytes
                read_incremental.append(total_read_increment)
            
            # Convert to MiB/s (multiply by 2 for 500ms epochs)
            write_throughputs = [blocks * BLOCK_SIZE / 1024 / 1024 * 2 for blocks in write_incremental]
            read_throughputs = [bytes_read / 1024 / 1024 * 2 for bytes_read in read_incremental]
            
            peak_write = max(write_throughputs) if write_throughputs else 0
            peak_read = max(read_throughputs) if read_throughputs else 0
            
            # Cache metrics
            cache_hits = final_betree['cache']['hits']
            cache_misses = final_betree['cache']['misses']
            total_cache_requests = cache_hits + cache_misses
            cache_hit_rate = (cache_hits / total_cache_requests) * 100 if total_cache_requests > 0 else 0
            
            # System metrics
            peak_memory_mb = max([entry['proc_rss'] for entry in out_data]) / 1024 / 1024
            total_cpu_time = final_out['proc_utime'] + final_out['proc_stime']
            cpu_utilization = (total_cpu_time / runtime_sec) * 100
            
            # Storage utilization
            storage_used = final_betree['usage'][0]['total'] - final_betree['usage'][0]['free']
            storage_total = final_betree['usage'][0]['total']
            storage_utilization = (storage_used / storage_total) * 100 if storage_total > 0 else 0
            
            # IOPS (including both storage and memory operations)
            total_iops = (total_read + total_written + total_memory_read_count) / runtime_sec
            
            # Compression effectiveness calculation (percentage of space saved)
            if bytes_to_compressed > 0:
                # Calculate bytes saved by compression
                compression_bytes_saved = bytes_to_compressed - compressed_bytes
                compression_effectiveness = (compression_bytes_saved / bytes_to_compressed) * 100
                # Calculate effective physical blocks written (accounting for compression)
                # This estimates the actual storage blocks used after compression
                compression_ratio = compressed_bytes / bytes_to_compressed
                estimated_compressed_data_blocks = int((bytes_to_compressed * compression_ratio) / BLOCK_SIZE)
                # Add metadata/system blocks (difference between total written and estimated data blocks)
                estimated_data_blocks_before_compression = int(bytes_to_compressed / BLOCK_SIZE)
                metadata_blocks = max(0, total_written - estimated_data_blocks_before_compression)
                effective_blocks_written = estimated_compressed_data_blocks + metadata_blocks
            else:
                compression_effectiveness = 0  # No compression data available (none compression case)
                effective_blocks_written = total_written  # No compression, use actual written blocks
            
            return {
                'avg_write_throughput_mbps': round(avg_write_throughput, 2),
                'avg_read_throughput_mbps': round(avg_read_throughput, 2),
                'peak_write_throughput_mbps': round(peak_write, 2),
                'peak_read_throughput_mbps': round(peak_read, 2),
                'total_data_written_mb': round(total_written * BLOCK_SIZE / 1024 / 1024, 4),
                'total_data_read_mb': round(total_read_bytes / 1024 / 1024, 4),
                'total_data_written_blocks': int(total_written),  # Raw logical block count as integer
                'total_data_read_blocks': int(total_read),        # Raw logical block count as integer
                'effective_blocks_written': int(effective_blocks_written),  # Physical blocks after compression
                'cache_hit_rate_percent': round(cache_hit_rate, 2),
                'peak_memory_mb': round(peak_memory_mb, 2),
                'cpu_utilization_percent': round(cpu_utilization, 2),
                'storage_utilization_percent': round(storage_utilization, 2),
                'total_iops': round(total_iops, 2),
                'compression_effectiveness_percent': round(compression_effectiveness, 1),
                'compression_time_seconds': round(total_compression_time_ns / 1_000_000_000, 3),  # Convert ns to seconds
                'decompression_time_seconds': round(total_decompression_time_ns / 1_000_000_000, 3),  # Convert ns to seconds
                'runtime_seconds': round(runtime_sec, 2)
            }
            
        except (json.JSONDecodeError, KeyError, IndexError, ZeroDivisionError) as e:
            print(f"Error processing {folder_path}: {e}")
            return {}
    
    def collect_data(self):
        """Collect data from all benchmark runs"""
        print("Collecting benchmark data...")
        
        for folder in self.results_dir.iterdir():
            if not folder.is_dir() or not folder.name.startswith(f'ycsb_{self.ycsb_char}_entry'):
                continue
            
            # Parse folder name
            parsed = self.parse_folder_name(folder.name)
            if not parsed:
                continue
                
            entry_size, compression, timestamp = parsed
            
            # Get thread count
            thread_count = self.extract_thread_count(folder)
            if thread_count is None:
                continue
            
            # Verify compression from config if needed
            if compression not in self.compression_types:
                compression = self.extract_compression_from_config(folder)
                if compression is None or compression not in self.compression_types:
                    continue
            
            # Calculate metrics
            metrics = self.calculate_metrics(folder)
            if not metrics:
                continue
            
            # Store data
            key = (entry_size, compression, thread_count)
            if key not in self.data:
                self.data[key] = []
            self.data[key].append(metrics)
            
            print(f"Processed: {folder.name} -> Entry:{entry_size}, Compression:{compression}, Threads:{thread_count}")
    
    def aggregate_data(self):
        """Aggregate multiple runs for the same configuration"""
        print("Aggregating data...")
        
        for key, runs in self.data.items():
            if len(runs) == 1:
                self.metrics[key] = runs[0]
            else:
                # Average multiple runs
                aggregated = {}
                for metric_name in runs[0].keys():
                    values = [run[metric_name] for run in runs if metric_name in run]
                    if values:
                        # Use different decimal places for different metrics
                        if metric_name in ['total_data_written_blocks', 'total_data_read_blocks', 'effective_blocks_written']:
                            # Block counts should be integers
                            aggregated[metric_name] = int(round(np.mean(values)))
                        elif metric_name in ['total_data_written_mb', 'total_data_read_mb']:
                            decimal_places = 4
                            aggregated[metric_name] = round(np.mean(values), decimal_places)
                        elif metric_name == 'compression_effectiveness_percent':
                            decimal_places = 1
                            aggregated[metric_name] = round(np.mean(values), decimal_places)
                        else:
                            decimal_places = 2
                            aggregated[metric_name] = round(np.mean(values), decimal_places)
                self.metrics[key] = aggregated
    
    def create_heatmap_data(self, metric_name: str, remove_empty: bool = None) -> Dict[int, pd.DataFrame]:
        """Create heatmap data organized by entry size"""
        if remove_empty is None:
            remove_empty = self.remove_empty
            
        heatmap_data = {}
        
        for entry_size in self.entry_sizes:
            # Create DataFrame for this entry size
            data_matrix = []
            row_labels = []
            
            for compression in self.compression_types:
                row_data = []
                for thread_count in self.thread_counts:
                    key = (entry_size, compression, thread_count)
                    if key in self.metrics and metric_name in self.metrics[key]:
                        value = self.metrics[key][metric_name]
                    else:
                        value = np.nan
                    row_data.append(value)
                
                data_matrix.append(row_data)
                row_labels.append(f"{entry_size}B_{compression}")
            
            df = pd.DataFrame(data_matrix, 
                            index=row_labels, 
                            columns=self.thread_counts)
            
            if remove_empty:
                # Remove rows (compression types) that are completely empty
                df = df.dropna(how='all')
                
                # Remove columns (thread counts) that are completely empty
                # Use a more compatible approach for older pandas versions
                cols_to_keep = []
                for col in df.columns:
                    if not df[col].isna().all():
                        cols_to_keep.append(col)
                df = df[cols_to_keep]
            
            # Only add to heatmap_data if there's actual data (or if keeping empty and df exists)
            if not df.empty or not remove_empty:
                heatmap_data[entry_size] = df
        
        return heatmap_data
    
    def plot_heatmap(self, metric_name: str, title: str, unit: str = "", cmap: str = 'viridis'):
        """Create and save heatmap for a specific metric"""
        heatmap_data = self.create_heatmap_data(metric_name)
        
        # Filter out entry sizes with no data
        available_entry_sizes = [size for size in self.entry_sizes if size in heatmap_data]
        
        if not available_entry_sizes:
            print(f"No data available for metric '{metric_name}'. Skipping heatmap generation.")
            return
        
        # Create figure with subplots for each entry size that has data
        fig, axes = plt.subplots(len(available_entry_sizes), 1, figsize=(12, 6 * len(available_entry_sizes)))
        if len(available_entry_sizes) == 1:
            axes = [axes]
        
        for i, entry_size in enumerate(available_entry_sizes):
            df = heatmap_data[entry_size]
            
            # Use separate color scale for each entry size
            vmin = df.min().min() if not df.isna().all().all() else 0
            vmax = df.max().max() if not df.isna().all().all() else 1
            
            # Create heatmap with appropriate formatting
            # Use different decimal places for different metrics
            if metric_name in ['total_data_written_blocks', 'total_data_read_blocks', 'effective_blocks_written']:
                fmt = '.0f'  # Integer format for block counts (no decimal places)
            elif metric_name in ['total_data_written_mb', 'total_data_read_mb']:
                fmt = '.4f'  # 4 decimal places for data written/read in MB
            elif metric_name == 'compression_effectiveness_percent':
                fmt = '.1f'  # 1 decimal place for compression effectiveness
            else:
                fmt = '.1f'  # 1 decimal place for other metrics
            sns.heatmap(df, 
                       ax=axes[i],
                       annot=True, 
                       fmt=fmt,
                       cmap=cmap,
                       vmin=vmin,
                       vmax=vmax,
                       cbar_kws={'label': unit},
                       xticklabels=True,
                       yticklabels=True)
            
            axes[i].set_title(f'{title} - Entry Size: {entry_size}B')
            axes[i].set_xlabel('Thread Count')
            axes[i].set_ylabel('Compression Type')
        
        plt.tight_layout()
        
        # Save heatmap
        output_file = self.results_dir / f'ycsb_{self.ycsb_char}_heatmap_{metric_name}.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved heatmap: {output_file}")
    
    def generate_all_heatmaps(self):
        """Generate heatmaps for all metrics"""
        print("Generating heatmaps...")
        
        # Define metrics to plot
        metrics_config = [
            ('avg_write_throughput_mbps', 'Average Write Throughput', 'MiB/s', 'Reds'),
            ('avg_read_throughput_mbps', 'Average Read Throughput', 'MiB/s', 'Blues'),
            ('peak_write_throughput_mbps', 'Peak Write Throughput', 'MiB/s', 'Reds'),
            ('peak_read_throughput_mbps', 'Peak Read Throughput', 'MiB/s', 'Blues'),
            ('total_data_written_mb', 'Total Data Written', 'MB', 'Oranges'),
            ('total_data_read_mb', 'Total Data Read', 'MB', 'Purples'),
            ('total_data_written_blocks', 'Total Data Written (Logical)', 'blocks', 'Oranges'),
            ('total_data_read_blocks', 'Total Data Read (Logical)', 'blocks', 'Purples'),
            ('effective_blocks_written', 'Effective Blocks Written (After Compression)', 'blocks', 'Reds'),
            ('cache_hit_rate_percent', 'Cache Hit Rate', '%', 'Greens'),
            ('peak_memory_mb', 'Peak Memory Usage', 'MB', 'YlOrRd'),
            ('cpu_utilization_percent', 'CPU Utilization', '%', 'plasma'),
            ('storage_utilization_percent', 'Storage Utilization', '%', 'viridis'),
            ('total_iops', 'Total IOPS', 'ops/s', 'magma'),
            ('compression_effectiveness_percent', 'Compression Effectiveness', '%', 'RdYlGn'),
            ('compression_time_seconds', 'Total Compression Time', 'seconds', 'Reds'),
            ('decompression_time_seconds', 'Total Decompression Time', 'seconds', 'Blues'),
            ('runtime_seconds', 'Runtime', 'seconds', 'coolwarm')
        ]
        
        for metric_name, title, unit, cmap in metrics_config:
            self.plot_heatmap(metric_name, title, unit, cmap)
    
    def generate_summary_report(self):
        """Generate a summary report of the collected data"""
        report_file = self.results_dir / 'benchmark_summary_report.txt'
        
        with open(report_file, 'w') as f:
            f.write("Haura Benchmark Results Summary\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Total configurations processed: {len(self.metrics)}\n")
            f.write(f"Results directory: {self.results_dir}\n\n")
            
            # Count by entry size
            f.write("Configurations by Entry Size:\n")
            for entry_size in self.entry_sizes:
                count = sum(1 for key in self.metrics.keys() if key[0] == entry_size)
                f.write(f"  {entry_size}B: {count} configurations\n")
            
            f.write("\nConfigurations by Compression:\n")
            for compression in self.compression_types:
                count = sum(1 for key in self.metrics.keys() if key[1] == compression)
                f.write(f"  {compression}: {count} configurations\n")
            
            f.write("\nConfigurations by Thread Count:\n")
            for thread_count in self.thread_counts:
                count = sum(1 for key in self.metrics.keys() if key[2] == thread_count)
                f.write(f"  {thread_count} threads: {count} configurations\n")
            
            # Best performers
            if self.metrics:
                f.write("\nTop Performers:\n")
                
                # Best average write throughput
                best_write = max(self.metrics.items(), 
                               key=lambda x: x[1].get('avg_write_throughput_mbps', 0))
                f.write(f"Best Avg Write Throughput: {best_write[1]['avg_write_throughput_mbps']} MiB/s ")
                f.write(f"(Entry:{best_write[0][0]}B, {best_write[0][1]}, {best_write[0][2]} threads)\n")
                
                # Best average read throughput
                best_read = max(self.metrics.items(), 
                              key=lambda x: x[1].get('avg_read_throughput_mbps', 0))
                f.write(f"Best Avg Read Throughput: {best_read[1]['avg_read_throughput_mbps']} MiB/s ")
                f.write(f"(Entry:{best_read[0][0]}B, {best_read[0][1]}, {best_read[0][2]} threads)\n")
                
                # Best cache hit rate
                best_cache = max(self.metrics.items(), 
                               key=lambda x: x[1].get('cache_hit_rate_percent', 0))
                f.write(f"Best Cache Hit Rate: {best_cache[1]['cache_hit_rate_percent']}% ")
                f.write(f"(Entry:{best_cache[0][0]}B, {best_cache[0][1]}, {best_cache[0][2]} threads)\n")
        
        print(f"Summary report saved: {report_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate heatmaps for Haura benchmark results')
    parser.add_argument('results_dir', 
                       help='Path to benchmark results directory (e.g., /path/to/2025-07-24_default)')
    parser.add_argument('ycsb_char', 
                       help='YCSB workload character (e.g., a, b, c, d, g)')
    parser.add_argument('--keep-empty', action='store_true',
                       help='Keep empty rows and columns in heatmaps (default: remove them)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.results_dir):
        print(f"Error: Results directory '{args.results_dir}' does not exist")
        return 1
    
    # Create heatmap generator
    generator = BenchmarkHeatmapGenerator(args.results_dir, args.ycsb_char, remove_empty=not args.keep_empty)
    
    # Process data
    generator.collect_data()
    generator.aggregate_data()
    
    # Generate outputs
    generator.generate_all_heatmaps()
    generator.generate_summary_report()
    
    print(f"\nHeatmap generation complete! Check {args.results_dir} for output files.")
    return 0

if __name__ == '__main__':
    exit(main())