#!/usr/bin/env python3
"""
Integrated heatmap analyzer that works with existing Haura plotting infrastructure
Provides both command-line interface and programmatic API
"""

import os
import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import argparse

class IntegratedHauraBenchmarkAnalyzer:
    """
    Enhanced analyzer that integrates with existing Haura benchmark infrastructure
    """
    
    def __init__(self, results_dir: str):
        self.results_dir = Path(results_dir)
        self.data = {}
        self.compression_algorithms = set()
        self.entry_sizes = set()
        self.thread_counts = set()
        self.metrics_cache = {}
        
    def parse_directory_name(self, dir_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse directory name to extract entry size, compression, and timestamp"""
        # Pattern: ycsb_g_entry{size}_{compression}_{timestamp}
        pattern = r'ycsb_g_entry(\d+)_([^_]+(?:_\d+)?)_(\d+)'
        match = re.match(pattern, dir_name)
        
        if not match:
            return None, None, None
            
        entry_size, compression, timestamp = match.groups()
        
        # Normalize compression names for better display
        compression_map = {
            'none': 'None',
            'snappy': 'Snappy',
            'lz4_1': 'Lz4(1)',
            'lz4_5': 'Lz4(5)', 
            'lz4_10': 'Lz4(10)',
            'zstd1': 'Zstd(1)',
            'zstd5': 'Zstd(5)',
            'zstd10': 'Zstd(10)'
        }
        
        compression = compression_map.get(compression, compression)
        
        return entry_size, compression, timestamp
    
    def load_ycsb_data(self, run_dir: Path) -> Optional[Dict[str, Any]]:
        """Load YCSB performance data from CSV"""
        csv_file = run_dir / 'ycsb_g.csv'
        if not csv_file.exists():
            return None
            
        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
            if len(rows) == 0:
                return None
                
            row = rows[0]
            threads = int(row['threads'])
            ops = int(row['ops'])
            time_ns = int(row['time_ns'])
            
            # Calculate derived metrics
            throughput = ops / (time_ns / 1e9)  # ops/sec
            latency_ns = time_ns / ops  # avg latency per op
            
            return {
                'threads': threads,
                'ops': ops,
                'time_ns': time_ns,
                'throughput': throughput,
                'avg_latency_ns': latency_ns,
                'avg_latency_ms': latency_ns / 1e6
            }
        except Exception as e:
            print(f"Warning: Error loading {csv_file}: {e}")
            return None
    
    def load_metrics_data(self, run_dir: Path) -> Dict[str, Any]:
        """Load betree metrics from JSONL with enhanced analysis"""
        metrics_file = run_dir / 'betree-metrics.jsonl'
        if not metrics_file.exists():
            return {}
            
        try:
            metrics = []
            with open(metrics_file, 'r') as f:
                for line in f:
                    if line.strip():
                        metrics.append(json.loads(line))
            
            if not metrics:
                return {}
            
            # Analyze metrics over time
            final_metrics = metrics[-1]  # Last measurement
            initial_metrics = metrics[0]  # First measurement
            
            # Cache analysis
            cache_data = final_metrics.get('cache', {})
            cache_hits = cache_data.get('hits', 0)
            cache_misses = cache_data.get('misses', 0)
            cache_total = cache_hits + cache_misses
            cache_hit_rate = cache_hits / cache_total if cache_total > 0 else 0
            cache_size = cache_data.get('size', 0)
            cache_capacity = cache_data.get('capacity', 0)
            cache_utilization = cache_size / cache_capacity if cache_capacity > 0 else 0
            
            # Storage analysis
            storage_data = final_metrics.get('storage', {})
            total_written = 0
            total_read = 0
            failed_reads = 0
            failed_writes = 0
            checksum_errors = 0
            
            if storage_data.get('tiers'):
                for tier in storage_data['tiers']:
                    for vdev in tier.get('vdevs', []):
                        total_written += vdev.get('written', 0)
                        total_read += vdev.get('read', 0)
                        failed_reads += vdev.get('failed_reads', 0)
                        failed_writes += vdev.get('failed_writes', 0)
                        checksum_errors += vdev.get('checksum_errors', 0)
            
            # Storage usage analysis
            usage_data = final_metrics.get('usage', [])
            storage_used = 0
            storage_total = 0
            storage_free = 0
            
            if usage_data:
                for tier_usage in usage_data:
                    tier_total = tier_usage.get('total', 0)
                    tier_free = tier_usage.get('free', 0)
                    tier_used = tier_total - tier_free
                    
                    storage_used += tier_used
                    storage_total += tier_total
                    storage_free += tier_free
            
            storage_utilization = storage_used / storage_total if storage_total > 0 else 0
            
            # Calculate rates and efficiency metrics
            runtime_ms = final_metrics.get('epoch_ms', 0) - initial_metrics.get('epoch_ms', 0)
            runtime_s = runtime_ms / 1000 if runtime_ms > 0 else 1
            
            write_rate = total_written / runtime_s if runtime_s > 0 else 0
            read_rate = total_read / runtime_s if runtime_s > 0 else 0
            
            return {
                # Cache metrics
                'cache_hits': cache_hits,
                'cache_misses': cache_misses,
                'cache_hit_rate': cache_hit_rate,
                'cache_size': cache_size,
                'cache_utilization': cache_utilization,
                
                # Storage I/O metrics
                'storage_written': total_written,
                'storage_read': total_read,
                'write_rate_bytes_per_sec': write_rate,
                'read_rate_bytes_per_sec': read_rate,
                
                # Storage usage metrics
                'storage_used': storage_used,
                'storage_free': storage_free,
                'storage_total': storage_total,
                'storage_utilization': storage_utilization,
                
                # Error metrics
                'failed_reads': failed_reads,
                'failed_writes': failed_writes,
                'checksum_errors': checksum_errors,
                
                # Runtime metrics
                'runtime_ms': runtime_ms,
                'runtime_s': runtime_s
            }
            
        except Exception as e:
            print(f"Warning: Error loading {metrics_file}: {e}")
            return {}
    
    def load_all_data(self, verbose: bool = True):
        """Load all benchmark data from results directory"""
        if verbose:
            print(f"Loading data from {self.results_dir}")
        
        loaded_count = 0
        skipped_count = 0
        
        for run_dir in self.results_dir.iterdir():
            if not run_dir.is_dir():
                continue
                
            entry_size, compression, timestamp = self.parse_directory_name(run_dir.name)
            if not all([entry_size, compression, timestamp]):
                skipped_count += 1
                continue
            
            # Load performance data
            ycsb_data = self.load_ycsb_data(run_dir)
            if not ycsb_data:
                skipped_count += 1
                continue
                
            # Load metrics data
            metrics_data = self.load_metrics_data(run_dir)
            
            # Combine data
            combined_data = {
                **ycsb_data, 
                **metrics_data,
                'entry_size': entry_size,
                'compression': compression,
                'timestamp': timestamp,
                'run_dir': str(run_dir)
            }
            
            # Store in nested structure
            key = (compression, entry_size, ycsb_data['threads'])
            self.data[key] = combined_data
            
            # Track unique values
            self.compression_algorithms.add(compression)
            self.entry_sizes.add(entry_size)
            self.thread_counts.add(ycsb_data['threads'])
            
            loaded_count += 1
        
        # Convert to sorted lists
        self.compression_algorithms = sorted(list(self.compression_algorithms))
        self.entry_sizes = sorted(list(self.entry_sizes), key=int)
        self.thread_counts = sorted(list(self.thread_counts))
        
        if verbose:
            print(f"Loaded {loaded_count} benchmark runs")
            if skipped_count > 0:
                print(f"Skipped {skipped_count} directories")
            print(f"Found {len(self.compression_algorithms)} compression algorithms, {len(self.entry_sizes)} entry sizes, {len(self.thread_counts)} thread counts")
    
    def get_metric_statistics(self, metric: str) -> Dict[str, Any]:
        """Get statistics for a specific metric"""
        values = []
        for data in self.data.values():
            value = data.get(metric)
            if value is not None and not (isinstance(value, float) and (value != value)):  # Check for NaN
                values.append(value)
        
        if not values:
            return {'count': 0}
        
        values.sort()
        n = len(values)
        
        return {
            'count': n,
            'min': values[0],
            'max': values[-1],
            'mean': sum(values) / n,
            'median': values[n//2] if n % 2 == 1 else (values[n//2-1] + values[n//2]) / 2,
            'q25': values[n//4],
            'q75': values[3*n//4]
        }
    
    def find_optimal_configurations(self, metric: str, maximize: bool = True, top_n: int = 5) -> List[Tuple]:
        """Find optimal configurations for a given metric"""
        configs = []
        
        for key, data in self.data.items():
            value = data.get(metric)
            if value is not None and not (isinstance(value, float) and (value != value)):
                configs.append((key, value))
        
        # Sort by metric value
        configs.sort(key=lambda x: x[1], reverse=maximize)
        
        return configs[:top_n]
    
    def create_heatmap_data(self, metric: str) -> Tuple[List[List], List[str], List[str], List[List]]:
        """Create heatmap data matrix for a specific metric, grouped by entry size"""
        
        # Create row labels: grouped by entry size, then compression algorithms
        row_labels = []
        for entry_size in self.entry_sizes:
            for compression in self.compression_algorithms:
                row_labels.append(f"{compression}_{entry_size}B")
        
        # Column labels: thread counts
        col_labels = [f"{t} threads" for t in self.thread_counts]
        
        # Create data matrix - grouped by entry size
        data_matrix = []
        text_matrix = []
        
        for entry_size in self.entry_sizes:
            for compression in self.compression_algorithms:
                data_row = []
                text_row = []
                
                for thread_count in self.thread_counts:
                    key = (compression, entry_size, thread_count)
                    if key in self.data:
                        value = self.data[key].get(metric)
                        if value is not None and not (isinstance(value, float) and (value != value)):
                            data_row.append(value)
                            
                            # Format text based on metric type
                            if metric in ['throughput', 'write_rate_bytes_per_sec', 'read_rate_bytes_per_sec']:
                                text_row.append(f"{value:.0f}")
                            elif metric in ['cache_hit_rate', 'storage_utilization', 'cache_utilization']:
                                text_row.append(f"{value:.1%}")
                            elif metric in ['avg_latency_ms']:
                                text_row.append(f"{value:.2f}ms")
                            elif metric in ['avg_latency_ns']:
                                text_row.append(f"{value:.0f}ns")
                            elif metric in ['cache_hits', 'cache_misses', 'storage_written', 'storage_read', 'ops']:
                                if value >= 1e6:
                                    text_row.append(f"{value/1e6:.1f}M")
                                elif value >= 1e3:
                                    text_row.append(f"{value/1e3:.1f}K")
                                else:
                                    text_row.append(f"{value:.0f}")
                            else:
                                text_row.append(f"{value:.2f}")
                        else:
                            data_row.append(None)
                            text_row.append("N/A")
                    else:
                        data_row.append(None)
                        text_row.append("N/A")
                
                data_matrix.append(data_row)
                text_matrix.append(text_row)
        
        return data_matrix, row_labels, col_labels, text_matrix
    
    def create_plotly_heatmap(self, metric: str, title: str, colorscale: str = 'Viridis', reverse_scale: bool = False) -> str:
        """Create a Plotly heatmap JSON for a specific metric with separate subplots for each entry size"""
        
        if reverse_scale:
            colorscale = colorscale + '_r'
        
        # Create separate data for each entry size
        subplot_data = []
        subplot_titles = []
        
        for i, entry_size in enumerate(self.entry_sizes):
            # Get data for this entry size only
            entry_data_matrix = []
            entry_text_matrix = []
            entry_row_labels = []
            
            for compression in self.compression_algorithms:
                data_row = []
                text_row = []
                
                for thread_count in self.thread_counts:
                    key = (compression, entry_size, thread_count)
                    if key in self.data:
                        value = self.data[key].get(metric)
                        if value is not None and not (isinstance(value, float) and (value != value)):
                            data_row.append(value)
                            
                            # Format text based on metric type
                            if metric in ['throughput', 'write_rate_bytes_per_sec', 'read_rate_bytes_per_sec']:
                                text_row.append(f"{value:.0f}")
                            elif metric in ['cache_hit_rate', 'storage_utilization', 'cache_utilization']:
                                text_row.append(f"{value:.1%}")
                            elif metric in ['avg_latency_ms']:
                                text_row.append(f"{value:.2f}ms")
                            elif metric in ['avg_latency_ns']:
                                text_row.append(f"{value:.0f}ns")
                            elif metric in ['cache_hits', 'cache_misses', 'storage_written', 'storage_read', 'ops']:
                                if value >= 1e6:
                                    text_row.append(f"{value/1e6:.1f}M")
                                elif value >= 1e3:
                                    text_row.append(f"{value/1e3:.1f}K")
                                else:
                                    text_row.append(f"{value:.0f}")
                            else:
                                text_row.append(f"{value:.2f}")
                        else:
                            data_row.append(None)
                            text_row.append("N/A")
                    else:
                        data_row.append(None)
                        text_row.append("N/A")
                
                entry_data_matrix.append(data_row)
                entry_text_matrix.append(text_row)
                entry_row_labels.append(compression)
            
            # Column labels: thread counts
            col_labels = [f"{t} threads" for t in self.thread_counts]
            
            # Create heatmap trace for this entry size
            trace = {
                "type": "heatmap",
                "z": entry_data_matrix,
                "x": col_labels,
                "y": entry_row_labels,
                "text": entry_text_matrix,
                "texttemplate": "%{text}",
                "textfont": {"size": 10, "color": "white"},
                "colorscale": colorscale,
                "hoverongaps": False,
                "hovertemplate": f"<b>%{{y}} ({entry_size}B)</b><br>%{{x}}<br>" + title + ": %{z}<extra></extra>",
                "showscale": True,
                "colorbar": {
                    "x": 1.02 + i * 0.05,  # Position colorbars side by side
                    "len": 0.2,
                    "y": 0.8 - i * 0.25,   # Stack colorbars vertically
                    "title": f"{entry_size}B",
                    "titleside": "right"
                },
                "xaxis": f"x{i+1}" if i > 0 else "x",
                "yaxis": f"y{i+1}" if i > 0 else "y"
            }
            
            subplot_data.append(trace)
            subplot_titles.append(f"{entry_size}B Entries")
        
        # Create subplot layout
        rows = len(self.entry_sizes)
        subplot_height = 200  # Height per subplot
        total_height = rows * subplot_height + 100  # Add margin
        
        # Create layout with subplots
        layout = {
            "title": {
                "text": f"{title} Heatmap (Grouped by Entry Size)",
                "x": 0.5,
                "font": {"size": 16}
            },
            "width": 1000,
            "height": total_height,
            "font": {"size": 11},
            "margin": {"l": 120, "r": 150, "t": 80, "b": 50},
            "showlegend": False
        }
        
        # Add subplot-specific axis configurations
        for i, entry_size in enumerate(self.entry_sizes):
            y_position = 1 - (i + 0.5) / len(self.entry_sizes)
            y_height = 0.8 / len(self.entry_sizes)
            
            # X-axis configuration
            x_key = "xaxis" if i == 0 else f"xaxis{i+1}"
            layout[x_key] = {
                "domain": [0, 0.85],
                "anchor": f"y{i+1}" if i > 0 else "y",
                "title": "Thread Count" if i == len(self.entry_sizes) - 1 else "",
                "side": "bottom"
            }
            
            # Y-axis configuration  
            y_key = "yaxis" if i == 0 else f"yaxis{i+1}"
            layout[y_key] = {
                "domain": [y_position - y_height/2, y_position + y_height/2],
                "anchor": f"x{i+1}" if i > 0 else "x",
                "title": f"{entry_size}B",
                "autorange": "reversed",
                "side": "left"
            }
        
        # Create Plotly data structure
        heatmap_data = {
            "data": subplot_data,
            "layout": layout
        }
        
        return json.dumps(heatmap_data)
    
    def generate_comprehensive_report(self) -> str:
        """Generate a comprehensive HTML report with all heatmaps and analysis"""
        
        # Define metrics to visualize with enhanced configuration
        metrics_config = [
            {
                'metric': 'throughput',
                'title': 'Throughput (ops/sec)',
                'colorscale': 'Viridis',
                'reverse_scale': False,
                'description': 'Higher throughput indicates better performance. Shows operations completed per second.',
                'category': 'Performance'
            },
            {
                'metric': 'avg_latency_ms',
                'title': 'Average Latency (ms)',
                'colorscale': 'Reds',
                'reverse_scale': True,
                'description': 'Lower latency is better. Shows average time per operation in milliseconds.',
                'category': 'Performance'
            },
            {
                'metric': 'cache_hit_rate', 
                'title': 'Cache Hit Rate (%)',
                'colorscale': 'RdYlGn',
                'reverse_scale': False,
                'description': 'Higher cache hit rate is better. Shows percentage of requests served from cache.',
                'category': 'Cache'
            },
            {
                'metric': 'cache_utilization',
                'title': 'Cache Utilization (%)',
                'colorscale': 'Blues',
                'reverse_scale': False,
                'description': 'Shows how much of the available cache capacity is being used.',
                'category': 'Cache'
            },
            {
                'metric': 'storage_written',
                'title': 'Storage Written (bytes)',
                'colorscale': 'Oranges',
                'reverse_scale': True,
                'description': 'Lower storage writes indicate better compression efficiency.',
                'category': 'Storage'
            },
            {
                'metric': 'storage_utilization',
                'title': 'Storage Utilization (%)',
                'colorscale': 'Purples',
                'reverse_scale': True,
                'description': 'Lower storage utilization indicates better space efficiency.',
                'category': 'Storage'
            },
            {
                'metric': 'write_rate_bytes_per_sec',
                'title': 'Write Rate (bytes/sec)',
                'colorscale': 'Greens',
                'reverse_scale': False,
                'description': 'Storage write throughput in bytes per second.',
                'category': 'I/O'
            }
        ]
        
        # Generate statistics for key metrics
        stats_html = self._generate_statistics_section(metrics_config)
        
        # Generate optimal configurations section
        optimal_html = self._generate_optimal_configurations_section()
        
        # Create HTML content
        html_parts = [f"""
<!DOCTYPE html>
<html>
<head>
    <title>Haura Benchmark Comprehensive Analysis</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; 
            padding: 20px;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }}
        .header p {{
            margin: 10px 0 0 0;
            opacity: 0.9;
            font-size: 1.1em;
        }}
        .content {{
            padding: 30px;
        }}
        .metric-section {{ 
            margin: 40px 0; 
            border: 1px solid #e1e5e9; 
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        }}
        .metric-header {{
            background: #f8f9fa;
            padding: 20px;
            border-bottom: 1px solid #e1e5e9;
        }}
        .metric-section h2 {{ 
            color: #495057; 
            margin: 0;
            font-size: 1.5em;
            display: flex;
            align-items: center;
        }}
        .category-badge {{
            background: #007acc;
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.7em;
            margin-left: 15px;
            font-weight: 500;
        }}
        .chart-container {{ 
            padding: 20px;
            min-height: 700px;
        }}
        .description {{ 
            color: #6c757d;
            margin: 10px 0 0 0;
            font-style: italic;
        }}
        .summary {{
            background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
            border-radius: 8px;
            padding: 25px;
            margin: 30px 0;
            border-left: 5px solid #007acc;
        }}
        .data-info {{
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            padding: 20px;
            margin: 20px 0;
            border-radius: 8px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: white;
            border: 1px solid #e1e5e9;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }}
        .stat-card h4 {{
            margin: 0 0 15px 0;
            color: #495057;
            font-size: 1.1em;
        }}
        .stat-value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #007acc;
            margin: 5px 0;
        }}
        .optimal-config {{
            background: #d4edda;
            border: 1px solid #c3e6cb;
            border-radius: 5px;
            padding: 15px;
            margin: 10px 0;
        }}
        .optimal-config strong {{
            color: #155724;
        }}
        ul {{
            padding-left: 20px;
        }}
        li {{
            margin: 8px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Haura Benchmark Analysis</h1>
            <p>Comprehensive Multi-Dimensional Performance Heatmaps</p>
        </div>
        
        <div class="content">
            <div class="summary">
                <h3>Dataset Overview</h3>
                <p><strong>Total benchmark runs:</strong> {len(self.data)}</p>
                <p><strong>Compression algorithms:</strong> {", ".join(self.compression_algorithms)}</p>
                <p><strong>Entry sizes:</strong> {", ".join([f"{s}B" for s in self.entry_sizes])}</p>
                <p><strong>Thread counts:</strong> {", ".join([str(t) for t in self.thread_counts])}</p>
                <p><strong>Coverage:</strong> {len(self.data)} / {len(self.compression_algorithms) * len(self.entry_sizes) * len(self.thread_counts)} possible configurations ({100 * len(self.data) / (len(self.compression_algorithms) * len(self.entry_sizes) * len(self.thread_counts)):.1f}%)</p>
            </div>
            
            {stats_html}
            
            {optimal_html}
            
            <div class="data-info">
                <h3>How to Read These Heatmaps</h3>
                <ul>
                    <li><strong>Rows:</strong> Each row represents a compression algorithm + entry size combination</li>
                    <li><strong>Columns:</strong> Each column represents a different thread count</li>
                    <li><strong>Colors:</strong> Color intensity indicates performance level (varies by metric)</li>
                    <li><strong>Values:</strong> Hover over cells to see exact values and configuration details</li>
                    <li><strong>Comparison:</strong> Compare horizontally (thread scaling) or vertically (algorithm/size comparison)</li>
                    <li><strong>Missing data:</strong> Gray cells indicate missing benchmark data for that configuration</li>
                </ul>
            </div>
"""]
        
        # Generate each heatmap
        for config in metrics_config:
            try:
                heatmap_json = self.create_plotly_heatmap(
                    config['metric'],
                    config['title'],
                    config['colorscale'],
                    config['reverse_scale']
                )
                
                html_parts.append(f"""
            <div class="metric-section">
                <div class="metric-header">
                    <h2>{config['title']} <span class="category-badge">{config['category']}</span></h2>
                    <div class="description">
                        {config['description']}
                    </div>
                </div>
                <div id="{config['metric']}_heatmap" class="chart-container"></div>
            </div>
""")
                
                # Add the plotly JSON data
                html_parts.append(f"""
            <script>
                var {config['metric']}_data = {heatmap_json};
                Plotly.newPlot('{config['metric']}_heatmap', {config['metric']}_data.data, {config['metric']}_data.layout, {{responsive: true}});
            </script>
""")
                
            except Exception as e:
                print(f"Error creating heatmap for {config['metric']}: {e}")
                html_parts.append(f"""
            <div class="metric-section">
                <div class="metric-header">
                    <h2>ERROR: {config['title']}</h2>
                    <div class="description">
                        Error creating heatmap: {str(e)}
                    </div>
                </div>
            </div>
""")
        
        html_parts.append("""
        </div>
    </div>
</body>
</html>
""")
        
        return "".join(html_parts)
    
    def _generate_statistics_section(self, metrics_config: List[Dict]) -> str:
        """Generate statistics section HTML"""
        stats_cards = []
        
        for config in metrics_config[:4]:  # Show stats for first 4 metrics
            stats = self.get_metric_statistics(config['metric'])
            if stats['count'] > 0:
                stats_cards.append(f"""
                <div class="stat-card">
                    <h4>{config['title']}</h4>
                    <div class="stat-value">{stats['mean']:.2f}</div>
                    <div>Mean Value</div>
                    <div style="margin-top: 10px; font-size: 0.9em; color: #6c757d;">
                        Min: {stats['min']:.2f} | Max: {stats['max']:.2f}<br>
                        Median: {stats['median']:.2f} | Samples: {stats['count']}
                    </div>
                </div>
                """)
        
        return f"""
        <div class="summary">
            <h3>Key Metrics Statistics</h3>
            <div class="stats-grid">
                {"".join(stats_cards)}
            </div>
        </div>
        """
    
    def _generate_optimal_configurations_section(self) -> str:
        """Generate optimal configurations section HTML"""
        
        # Find optimal configs for key metrics
        optimal_throughput = self.find_optimal_configurations('throughput', maximize=True, top_n=3)
        optimal_latency = self.find_optimal_configurations('avg_latency_ms', maximize=False, top_n=3)
        optimal_cache = self.find_optimal_configurations('cache_hit_rate', maximize=True, top_n=3)
        
        def format_config(config_tuple):
            (compression, entry_size, threads), value = config_tuple
            return f"<strong>{compression}</strong> with <strong>{entry_size}B</strong> entries, <strong>{threads}</strong> threads"
        
        optimal_html = """
        <div class="summary">
            <h3>🏆 Optimal Configurations</h3>
        """
        
        if optimal_throughput:
            optimal_html += """
            <div class="optimal-config">
                <h4>🚀 Highest Throughput</h4>
            """
            for i, config in enumerate(optimal_throughput, 1):
                optimal_html += f"<p>{i}. {format_config(config)} → {config[1]:.0f} ops/sec</p>"
            optimal_html += "</div>"
        
        if optimal_latency:
            optimal_html += """
            <div class="optimal-config">
                <h4>⚡ Lowest Latency</h4>
            """
            for i, config in enumerate(optimal_latency, 1):
                optimal_html += f"<p>{i}. {format_config(config)} → {config[1]:.2f} ms</p>"
            optimal_html += "</div>"
        
        if optimal_cache:
            optimal_html += """
            <div class="optimal-config">
                <h4>🎯 Best Cache Performance</h4>
            """
            for i, config in enumerate(optimal_cache, 1):
                optimal_html += f"<p>{i}. {format_config(config)} → {config[1]:.1%} hit rate</p>"
            optimal_html += "</div>"
        
        optimal_html += "</div>"
        
        return optimal_html

def main():
    parser = argparse.ArgumentParser(description='Generate comprehensive heatmap analysis of Haura benchmark results')
    parser.add_argument('--results-dir', 
                       default='/home/skarim/Code/smash/haura/betree/haura-benchmarks/results/2025-07-24_default',
                       help='Path to benchmark results directory')
    parser.add_argument('--output', 
                       default='/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/comprehensive_analysis.html',
                       help='Output HTML file path')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress verbose output')
    
    args = parser.parse_args()
    
    # Create analyzer
    analyzer = IntegratedHauraBenchmarkAnalyzer(args.results_dir)
    
    # Load data
    analyzer.load_all_data(verbose=not args.quiet)
    
    if len(analyzer.data) == 0:
        print("❌ No benchmark data found!")
        return 1
    
    # Generate comprehensive report
    if not args.quiet:
        print("🌡️ Generating comprehensive analysis...")
    
    html_content = analyzer.generate_comprehensive_report()
    
    # Save HTML file
    with open(args.output, 'w') as f:
        f.write(html_content)
    
    if not args.quiet:
        print(f"✅ Comprehensive analysis saved to: {args.output}")
        print(f"📊 Generated analysis for {len(analyzer.data)} benchmark runs")
        print("🌐 Open the HTML file in a web browser to view the interactive analysis!")
    
    return 0

if __name__ == "__main__":
    exit(main())