#!/usr/bin/env python3
"""
Multi-dimensional heatmap analyzer for Haura benchmark results
Creates separate heatmaps for each metric across compression algorithms, entry sizes, and thread counts
"""

import os
import json
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any
import argparse

class HauraBenchmarkAnalyzer:
    def __init__(self, results_dir: str):
        self.results_dir = Path(results_dir)
        self.data = {}
        self.compression_algorithms = set()
        self.entry_sizes = set()
        self.thread_counts = set()
        
    def parse_directory_name(self, dir_name: str) -> Tuple[str, str, str]:
        """Parse directory name to extract entry size, compression, and timestamp"""
        # Pattern: ycsb_g_entry{size}_{compression}_{timestamp}
        pattern = r'ycsb_g_entry(\d+)_([^_]+(?:_\d+)?)_(\d+)'
        match = re.match(pattern, dir_name)
        
        if not match:
            return None, None, None
            
        entry_size, compression, timestamp = match.groups()
        
        # Normalize compression names
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
    
    def load_ycsb_data(self, run_dir: Path) -> Dict[str, Any]:
        """Load YCSB performance data from CSV"""
        csv_file = run_dir / 'ycsb_g.csv'
        if not csv_file.exists():
            return None
            
        try:
            df = pd.read_csv(csv_file)
            if len(df) == 0:
                return None
                
            row = df.iloc[0]
            threads = int(row['threads'])
            ops = int(row['ops'])
            time_ns = int(row['time_ns'])
            
            # Calculate throughput (ops/sec)
            throughput = ops / (time_ns / 1e9)
            
            return {
                'threads': threads,
                'ops': ops,
                'time_ns': time_ns,
                'throughput': throughput
            }
        except Exception as e:
            print(f"Error loading {csv_file}: {e}")
            return None
    
    def load_metrics_data(self, run_dir: Path) -> Dict[str, Any]:
        """Load betree metrics from JSONL"""
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
            
            # Calculate aggregate metrics
            final_metrics = metrics[-1]  # Last measurement
            
            cache_data = final_metrics.get('cache', {})
            storage_data = final_metrics.get('storage', {})
            usage_data = final_metrics.get('usage', [])
            
            # Cache metrics
            cache_hits = cache_data.get('hits', 0)
            cache_misses = cache_data.get('misses', 0)
            cache_hit_rate = cache_hits / (cache_hits + cache_misses) if (cache_hits + cache_misses) > 0 else 0
            
            # Storage metrics
            total_written = 0
            total_read = 0
            if storage_data.get('tiers'):
                for tier in storage_data['tiers']:
                    for vdev in tier.get('vdevs', []):
                        total_written += vdev.get('written', 0)
                        total_read += vdev.get('read', 0)
            
            # Storage usage
            storage_used = 0
            storage_total = 0
            if usage_data:
                for tier_usage in usage_data:
                    storage_used += tier_usage.get('total', 0) - tier_usage.get('free', 0)
                    storage_total += tier_usage.get('total', 0)
            
            storage_utilization = storage_used / storage_total if storage_total > 0 else 0
            
            return {
                'cache_hits': cache_hits,
                'cache_misses': cache_misses,
                'cache_hit_rate': cache_hit_rate,
                'storage_written': total_written,
                'storage_read': total_read,
                'storage_used': storage_used,
                'storage_utilization': storage_utilization
            }
            
        except Exception as e:
            print(f"Error loading {metrics_file}: {e}")
            return {}
    
    def load_all_data(self):
        """Load all benchmark data from results directory"""
        print(f"Loading data from {self.results_dir}")
        
        for run_dir in self.results_dir.iterdir():
            if not run_dir.is_dir():
                continue
                
            entry_size, compression, timestamp = self.parse_directory_name(run_dir.name)
            if not all([entry_size, compression, timestamp]):
                continue
            
            # Load performance data
            ycsb_data = self.load_ycsb_data(run_dir)
            if not ycsb_data:
                continue
                
            # Load metrics data
            metrics_data = self.load_metrics_data(run_dir)
            
            # Combine data
            combined_data = {**ycsb_data, **metrics_data}
            
            # Store in nested structure
            key = (compression, entry_size, ycsb_data['threads'])
            self.data[key] = combined_data
            
            # Track unique values
            self.compression_algorithms.add(compression)
            self.entry_sizes.add(entry_size)
            self.thread_counts.add(ycsb_data['threads'])
        
        # Convert to sorted lists
        self.compression_algorithms = sorted(list(self.compression_algorithms))
        self.entry_sizes = sorted(list(self.entry_sizes), key=int)
        self.thread_counts = sorted(list(self.thread_counts))
        
        print(f"Loaded {len(self.data)} benchmark runs")
        print(f"Compressions: {self.compression_algorithms}")
        print(f"Entry sizes: {self.entry_sizes}")
        print(f"Thread counts: {self.thread_counts}")
    
    def create_heatmap_data(self, metric: str) -> Tuple[np.ndarray, List[str], List[str]]:
        """Create heatmap data matrix for a specific metric"""
        
        # Create row labels: compression_entrysize
        row_labels = []
        for compression in self.compression_algorithms:
            for entry_size in self.entry_sizes:
                row_labels.append(f"{compression}_{entry_size}B")
        
        # Column labels: thread counts
        col_labels = [f"{t} threads" for t in self.thread_counts]
        
        # Create data matrix
        data_matrix = np.full((len(row_labels), len(col_labels)), np.nan)
        text_matrix = np.full((len(row_labels), len(col_labels)), "", dtype=object)
        
        for i, compression in enumerate(self.compression_algorithms):
            for j, entry_size in enumerate(self.entry_sizes):
                row_idx = i * len(self.entry_sizes) + j
                
                for k, thread_count in enumerate(self.thread_counts):
                    key = (compression, entry_size, thread_count)
                    if key in self.data:
                        value = self.data[key].get(metric, np.nan)
                        if not np.isnan(value):
                            data_matrix[row_idx, k] = value
                            
                            # Format text based on metric type
                            if metric == 'throughput':
                                text_matrix[row_idx, k] = f"{value:.0f}"
                            elif metric in ['cache_hit_rate', 'storage_utilization']:
                                text_matrix[row_idx, k] = f"{value:.2%}"
                            elif metric in ['cache_hits', 'cache_misses', 'storage_written', 'storage_read']:
                                text_matrix[row_idx, k] = f"{value:.0f}"
                            else:
                                text_matrix[row_idx, k] = f"{value:.2f}"
        
        return data_matrix, row_labels, col_labels, text_matrix
    
    def create_heatmap(self, metric: str, title: str, colorscale: str = 'Viridis', reverse_scale: bool = False) -> go.Figure:
        """Create a heatmap for a specific metric"""
        
        data_matrix, row_labels, col_labels, text_matrix = self.create_heatmap_data(metric)
        
        if reverse_scale:
            colorscale = colorscale + '_r'
        
        fig = go.Figure(data=go.Heatmap(
            z=data_matrix,
            x=col_labels,
            y=row_labels,
            text=text_matrix,
            texttemplate="%{text}",
            textfont={"size": 10, "color": "white"},
            colorscale=colorscale,
            hoverongaps=False,
            hovertemplate="<b>%{y}</b><br>%{x}<br>" + title + ": %{z}<extra></extra>"
        ))
        
        fig.update_layout(
            title=f"{title} Heatmap",
            xaxis_title="Thread Count",
            yaxis_title="Compression Algorithm & Entry Size",
            width=800,
            height=600,
            font=dict(size=12)
        )
        
        return fig
    
    def create_all_heatmaps(self) -> str:
        """Create all heatmaps and return HTML"""
        
        # Define metrics to visualize
        metrics_config = [
            {
                'metric': 'throughput',
                'title': 'Throughput (ops/sec)',
                'colorscale': 'Viridis',
                'reverse_scale': False,
                'description': 'Higher throughput is better (operations per second)'
            },
            {
                'metric': 'cache_hit_rate', 
                'title': 'Cache Hit Rate',
                'colorscale': 'RdYlGn',
                'reverse_scale': False,
                'description': 'Higher cache hit rate is better (percentage of cache hits)'
            },
            {
                'metric': 'cache_misses',
                'title': 'Cache Misses',
                'colorscale': 'Reds',
                'reverse_scale': True,
                'description': 'Lower cache misses are better (total number of cache misses)'
            },
            {
                'metric': 'storage_written',
                'title': 'Storage Written (bytes)',
                'colorscale': 'Blues',
                'reverse_scale': True,
                'description': 'Lower storage writes indicate better compression efficiency'
            },
            {
                'metric': 'storage_utilization',
                'title': 'Storage Utilization',
                'colorscale': 'Oranges',
                'reverse_scale': True,
                'description': 'Lower storage utilization indicates better compression'
            }
        ]
        
        # Create HTML content
        html_parts = ["""
<!DOCTYPE html>
<html>
<head>
    <title>Haura Benchmark Heatmaps</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 20px; 
            background-color: #f8f9fa;
        }
        .metric-section { 
            margin: 40px 0; 
            border: 2px solid #dee2e6; 
            padding: 25px; 
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-section h2 { 
            color: #495057; 
            border-bottom: 3px solid #007acc; 
            padding-bottom: 10px;
            margin-top: 0;
        }
        .chart-container { 
            margin: 20px 0; 
            min-height: 600px;
        }
        .description { 
            background: #e9ecef; 
            padding: 15px; 
            margin: 15px 0; 
            border-radius: 5px;
            border-left: 4px solid #007acc;
        }
        .summary {
            background: #d4edda;
            border: 1px solid #c3e6cb;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }
        .data-info {
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
        }
    </style>
</head>
<body>
    <h1>🌡️ Haura Benchmark Multi-Dimensional Heatmaps</h1>
    
    <div class="summary">
        <h3>📊 Data Overview</h3>
        <p><strong>Total benchmark runs:</strong> """ + str(len(self.data)) + """</p>
        <p><strong>Compression algorithms:</strong> """ + ", ".join(self.compression_algorithms) + """</p>
        <p><strong>Entry sizes:</strong> """ + ", ".join([f"{s}B" for s in self.entry_sizes]) + """</p>
        <p><strong>Thread counts:</strong> """ + ", ".join([str(t) for t in self.thread_counts]) + """</p>
    </div>
    
    <div class="data-info">
        <h3>🎯 How to Read These Heatmaps</h3>
        <ul>
            <li><strong>Rows:</strong> Each row represents a compression algorithm + entry size combination</li>
            <li><strong>Columns:</strong> Each column represents a different thread count</li>
            <li><strong>Colors:</strong> Darker/brighter colors indicate better performance (varies by metric)</li>
            <li><strong>Values:</strong> Hover over cells to see exact values</li>
            <li><strong>Comparison:</strong> Compare horizontally (same config, different threads) or vertically (same threads, different configs)</li>
        </ul>
    </div>
"""]
        
        # Generate each heatmap
        for config in metrics_config:
            try:
                fig = self.create_heatmap(
                    config['metric'],
                    config['title'],
                    config['colorscale'],
                    config['reverse_scale']
                )
                
                html_parts.append(f"""
    <div class="metric-section">
        <h2>🌡️ {config['title']}</h2>
        <div class="description">
            {config['description']}
        </div>
        <div id="{config['metric']}_heatmap" class="chart-container"></div>
    </div>
""")
                
                # Add the plotly JSON data
                html_parts.append(f"""
    <script>
        var {config['metric']}_data = {fig.to_json()};
        Plotly.newPlot('{config['metric']}_heatmap', {config['metric']}_data.data, {config['metric']}_data.layout, {{responsive: true}});
    </script>
""")
                
            except Exception as e:
                print(f"Error creating heatmap for {config['metric']}: {e}")
                html_parts.append(f"""
    <div class="metric-section">
        <h2>❌ {config['title']}</h2>
        <div class="description">
            Error creating heatmap: {str(e)}
        </div>
    </div>
""")
        
        html_parts.append("""
    <div class="summary">
        <h3>🎯 Key Insights</h3>
        <ul>
            <li><strong>Optimal configurations</strong> appear as dark/bright cells in performance heatmaps</li>
            <li><strong>Thread scaling</strong> can be observed by looking across rows (left to right)</li>
            <li><strong>Compression trade-offs</strong> are visible by comparing different compression algorithms</li>
            <li><strong>Entry size impact</strong> shows how data size affects each compression algorithm</li>
        </ul>
    </div>
</body>
</html>
""")
        
        return "".join(html_parts)

def main():
    parser = argparse.ArgumentParser(description='Generate heatmap analysis of Haura benchmark results')
    parser.add_argument('--results-dir', 
                       default='/home/skarim/Code/smash/haura/betree/haura-benchmarks/results/2025-07-24_default',
                       help='Path to benchmark results directory')
    parser.add_argument('--output', 
                       default='/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/heatmap_analysis.html',
                       help='Output HTML file path')
    
    args = parser.parse_args()
    
    # Create analyzer
    analyzer = HauraBenchmarkAnalyzer(args.results_dir)
    
    # Load data
    analyzer.load_all_data()
    
    if len(analyzer.data) == 0:
        print("❌ No benchmark data found!")
        return
    
    # Generate heatmaps
    print("🌡️ Generating heatmaps...")
    html_content = analyzer.create_all_heatmaps()
    
    # Save HTML file
    with open(args.output, 'w') as f:
        f.write(html_content)
    
    print(f"✅ Heatmap analysis saved to: {args.output}")
    print(f"📊 Generated heatmaps for {len(analyzer.data)} benchmark runs")
    print("🌐 Open the HTML file in a web browser to view the interactive heatmaps!")

if __name__ == "__main__":
    main()