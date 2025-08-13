#!/usr/bin/env python3
"""
Simple prototype implementations without complex dependencies
"""

import numpy as np
import json
import os

def generate_sample_data():
    """Generate realistic sample data mimicking YCSB benchmark results"""
    
    # Compression algorithms and their characteristics
    compressions = {
        'None': {'color': '#7f7f7f', 'cpu_overhead': 0, 'throughput_factor': 1.0},
        'Snappy': {'color': '#2ca02c', 'cpu_overhead': 5, 'throughput_factor': 0.95},
        'Zstd(1)': {'color': '#1f77b4', 'cpu_overhead': 15, 'throughput_factor': 0.85},
        'Zstd(5)': {'color': '#ff7f0e', 'cpu_overhead': 25, 'throughput_factor': 0.75},
        'Lz4(1)': {'color': '#d62728', 'cpu_overhead': 8, 'throughput_factor': 0.92}
    }
    
    entry_sizes = ['512', '4096', '16384', '30000']
    
    data = {}
    
    for compression, props in compressions.items():
        for entry_size in entry_sizes:
            # Generate 3 runs per configuration
            runs = []
            for run_id in range(3):
                # Time series (300 seconds, 1 second intervals)
                time_points = np.arange(0, 300, 1)
                
                # Base throughput with some variation
                base_throughput = 1000 + np.random.normal(0, 50)
                throughput = base_throughput * props['throughput_factor']
                
                # Add realistic patterns
                # Warmup phase
                warmup_factor = np.minimum(time_points / 30, 1.0)
                # Random variations
                noise = np.random.normal(0, throughput * 0.1, len(time_points))
                # Occasional spikes/drops
                spikes = np.random.choice([0, 1], len(time_points), p=[0.95, 0.05])
                spike_magnitude = np.random.normal(0, throughput * 0.3, len(time_points))
                
                final_throughput = throughput * warmup_factor + noise + spikes * spike_magnitude
                final_throughput = np.maximum(final_throughput, 0)  # No negative throughput
                
                # CPU usage
                base_cpu = 20 + props['cpu_overhead']
                cpu_usage = base_cpu + np.random.normal(0, 5, len(time_points))
                cpu_usage = np.clip(cpu_usage, 0, 100)
                
                # Memory usage (gradually increasing)
                memory_base = 500 + int(entry_size) * 0.1
                memory_usage = memory_base + time_points * 0.5 + np.random.normal(0, 20, len(time_points))
                
                # Storage tier usage
                tier_usage = np.cumsum(np.random.exponential(2, len(time_points))) + 1000
                
                # Cache misses (inversely related to throughput)
                cache_misses = (2000 - final_throughput/2) + np.random.normal(0, 100, len(time_points))
                cache_misses = np.maximum(cache_misses, 0)
                
                runs.append({
                    'run_id': run_id,
                    'time': time_points.tolist(),
                    'throughput': final_throughput.tolist(),
                    'cpu_usage': cpu_usage.tolist(),
                    'memory_usage': memory_usage.tolist(),
                    'tier_usage': tier_usage.tolist(),
                    'cache_misses': cache_misses.tolist(),
                    'peak_throughput': float(np.max(final_throughput)),
                    'avg_cpu': float(np.mean(cpu_usage)),
                    'total_cache_misses': float(np.sum(cache_misses))
                })
            
            data[(compression, entry_size)] = runs
    
    return data, compressions

def create_html_visualization(data, compressions):
    """Create HTML visualization with JavaScript charts"""
    
    html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>Metrics Visualization Prototypes</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .approach { margin: 40px 0; border: 1px solid #ccc; padding: 20px; }
        .approach h2 { color: #333; border-bottom: 2px solid #007acc; }
        .chart-container { margin: 20px 0; }
        .description { background: #f5f5f5; padding: 15px; margin: 10px 0; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; }
    </style>
</head>
<body>
    <h1>Metrics Visualization Prototypes</h1>
    <p>These are different approaches to visualize benchmark metrics while preserving individual run details.</p>
"""
    
    # Approach 1: Small Multiples
    html_content += """
    <div class="approach">
        <h2>Approach 1: Small Multiples with Run Overlays</h2>
        <div class="description">
            <strong>Benefits:</strong> Direct comparison between compression algorithms, individual run patterns visible, easy to spot outliers<br>
            <strong>Use Case:</strong> Time-series data like throughput, CPU usage, memory over time
        </div>
        <div class="grid">
"""
    
    entry_size = '4096'
    for compression in list(compressions.keys())[:4]:  # Show first 4
        runs = data.get((compression, entry_size), [])
        if runs:
            html_content += f'<div id="small_multiple_{compression.replace("(", "_").replace(")", "_")}" class="chart-container"></div>'
    
    html_content += """
        </div>
    </div>
"""
    
    # Approach 2: Grouped Lines
    html_content += """
    <div class="approach">
        <h2>Approach 2: Individual Lines with Group Clustering</h2>
        <div class="description">
            <strong>Benefits:</strong> All algorithms on same timeline, individual variations visible, group averages clear<br>
            <strong>Use Case:</strong> Comparing performance patterns across algorithms
        </div>
        <div id="grouped_lines" class="chart-container"></div>
    </div>
"""
    
    # Approach 3: Box Plots
    html_content += """
    <div class="approach">
        <h2>Approach 3: Box Plots with Individual Points</h2>
        <div class="description">
            <strong>Benefits:</strong> Statistical distribution visible, outliers clearly marked, easy comparison<br>
            <strong>Use Case:</strong> Summary statistics like peak performance, total resource usage
        </div>
        <div class="grid-3">
            <div id="boxplot_throughput" class="chart-container"></div>
            <div id="boxplot_cpu" class="chart-container"></div>
            <div id="boxplot_cache" class="chart-container"></div>
        </div>
    </div>
"""
    
    # Approach 4: Heatmap
    html_content += """
    <div class="approach">
        <h2>Approach 4: Heatmap with Details</h2>
        <div class="description">
            <strong>Benefits:</strong> Overview of all configurations, color-coded performance, compact representation<br>
            <strong>Use Case:</strong> Multi-dimensional comparisons across entry sizes and algorithms
        </div>
        <div class="grid-3">
            <div id="heatmap_throughput" class="chart-container"></div>
            <div id="heatmap_cpu" class="chart-container"></div>
            <div id="heatmap_cache" class="chart-container"></div>
        </div>
    </div>
"""
    
    # Approach 5: Layered
    html_content += """
    <div class="approach">
        <h2>Approach 5: Layered Visualization (Progressive Detail)</h2>
        <div class="description">
            <strong>Benefits:</strong> Start simple, add detail on demand, avoid visual clutter<br>
            <strong>Use Case:</strong> Interactive exploration of data
        </div>
        <div class="grid">
            <div id="layer_overview" class="chart-container"></div>
            <div id="layer_confidence" class="chart-container"></div>
            <div id="layer_selected" class="chart-container"></div>
            <div id="layer_full" class="chart-container"></div>
        </div>
    </div>
"""
    
    # Approach 6: Statistical Summary
    html_content += """
    <div class="approach">
        <h2>Approach 6: Statistical Summary with Details</h2>
        <div class="description">
            <strong>Benefits:</strong> Main trends + detailed breakdowns, performance ranking, comprehensive view<br>
            <strong>Use Case:</strong> Executive summary with drill-down capability
        </div>
        <div class="grid">
            <div id="summary_main" class="chart-container"></div>
            <div id="summary_ranking" class="chart-container"></div>
        </div>
    </div>
"""
    
    # JavaScript for charts
    html_content += """
    <script>
    // Data
    var data = """ + json.dumps(data, default=str) + """;
    var compressions = """ + json.dumps(compressions) + """;
    
    // Helper function to convert data structure
    function getRunsData(compression, entrySize) {
        var key = compression + ',' + entrySize;
        for (var k in data) {
            if (k.includes(compression) && k.includes(entrySize)) {
                return data[k];
            }
        }
        return [];
    }
    
    // Approach 1: Small Multiples
    var entrySize = '4096';
    var compressionList = Object.keys(compressions).slice(0, 4);
    
    compressionList.forEach(function(compression) {
        var runs = getRunsData(compression, entrySize);
        if (runs.length === 0) return;
        
        var traces = [];
        
        // Individual runs (thin lines)
        runs.forEach(function(run, idx) {
            traces.push({
                x: run.time,
                y: run.throughput,
                type: 'scatter',
                mode: 'lines',
                name: 'Run ' + (idx + 1),
                line: { color: compressions[compression].color, width: 1 },
                opacity: 0.4
            });
        });
        
        // Average (thick line)
        var avgThroughput = runs[0].time.map(function(_, i) {
            return runs.reduce(function(sum, run) {
                return sum + run.throughput[i];
            }, 0) / runs.length;
        });
        
        traces.push({
            x: runs[0].time,
            y: avgThroughput,
            type: 'scatter',
            mode: 'lines',
            name: 'Average',
            line: { color: compressions[compression].color, width: 3 }
        });
        
        var layout = {
            title: compression + ' - Entry ' + entrySize + 'B',
            xaxis: { title: 'Time (seconds)' },
            yaxis: { title: 'Throughput (ops/sec)' },
            showlegend: false
        };
        
        var divId = 'small_multiple_' + compression.replace(/[()]/g, '_');
        Plotly.newPlot(divId, traces, layout);
    });
    
    // Approach 2: Grouped Lines
    var groupedTraces = [];
    Object.keys(compressions).forEach(function(compression) {
        var runs = getRunsData(compression, entrySize);
        if (runs.length === 0) return;
        
        // Individual runs
        runs.forEach(function(run, idx) {
            groupedTraces.push({
                x: run.time,
                y: run.throughput,
                type: 'scatter',
                mode: 'lines',
                name: compression + ' Run ' + (idx + 1),
                line: { color: compressions[compression].color, width: 1, dash: 'dot' },
                opacity: 0.3,
                showlegend: idx === 0
            });
        });
        
        // Average
        var avgThroughput = runs[0].time.map(function(_, i) {
            return runs.reduce(function(sum, run) {
                return sum + run.throughput[i];
            }, 0) / runs.length;
        });
        
        groupedTraces.push({
            x: runs[0].time,
            y: avgThroughput,
            type: 'scatter',
            mode: 'lines',
            name: compression + ' Average',
            line: { color: compressions[compression].color, width: 3 }
        });
    });
    
    var groupedLayout = {
        title: 'Group Clustering: Throughput - Entry ' + entrySize + 'B',
        xaxis: { title: 'Time (seconds)' },
        yaxis: { title: 'Throughput (ops/sec)' }
    };
    
    Plotly.newPlot('grouped_lines', groupedTraces, groupedLayout);
    
    // Approach 3: Box Plots
    var metrics = [
        { key: 'peak_throughput', title: 'Peak Throughput', div: 'boxplot_throughput' },
        { key: 'avg_cpu', title: 'Average CPU %', div: 'boxplot_cpu' },
        { key: 'total_cache_misses', title: 'Total Cache Misses', div: 'boxplot_cache' }
    ];
    
    metrics.forEach(function(metric) {
        var boxTraces = [];
        Object.keys(compressions).forEach(function(compression) {
            var runs = getRunsData(compression, entrySize);
            if (runs.length === 0) return;
            
            var values = runs.map(function(run) { return run[metric.key]; });
            
            boxTraces.push({
                y: values,
                type: 'box',
                name: compression,
                marker: { color: compressions[compression].color }
            });
        });
        
        var boxLayout = {
            title: metric.title,
            yaxis: { title: metric.title }
        };
        
        Plotly.newPlot(metric.div, boxTraces, boxLayout);
    });
    
    // Approach 4: Heatmaps
    var entrySizes = ['512', '4096', '16384', '30000'];
    var compressionNames = Object.keys(compressions);
    
    metrics.forEach(function(metric) {
        var zData = [];
        var textData = [];
        
        compressionNames.forEach(function(compression) {
            var row = [];
            var textRow = [];
            entrySizes.forEach(function(entrySize) {
                var runs = getRunsData(compression, entrySize);
                if (runs.length > 0) {
                    var values = runs.map(function(run) { return run[metric.key]; });
                    var mean = values.reduce(function(a, b) { return a + b; }) / values.length;
                    var std = Math.sqrt(values.reduce(function(sum, val) {
                        return sum + Math.pow(val - mean, 2);
                    }, 0) / values.length);
                    row.push(mean);
                    textRow.push(mean.toFixed(0) + '±' + std.toFixed(0));
                } else {
                    row.push(0);
                    textRow.push('N/A');
                }
            });
            zData.push(row);
            textData.push(textRow);
        });
        
        var heatmapTrace = {
            z: zData,
            text: textData,
            texttemplate: '%{text}',
            textfont: { color: 'white' },
            type: 'heatmap',
            colorscale: 'Viridis',
            x: entrySizes,
            y: compressionNames
        };
        
        var heatmapLayout = {
            title: metric.title,
            xaxis: { title: 'Entry Size (bytes)' },
            yaxis: { title: 'Compression Algorithm' }
        };
        
        var heatmapDiv = 'heatmap_' + metric.key.split('_')[metric.key.split('_').length - 1];
        Plotly.newPlot(heatmapDiv, [heatmapTrace], heatmapLayout);
    });
    
    // Approach 5: Layered Visualization
    var selectedAlgos = ['None', 'Snappy', 'Zstd(1)'];
    
    // Layer 1: Overview (averages only)
    var overviewTraces = [];
    Object.keys(compressions).forEach(function(compression) {
        var runs = getRunsData(compression, entrySize);
        if (runs.length === 0) return;
        
        var avgThroughput = runs[0].time.map(function(_, i) {
            return runs.reduce(function(sum, run) {
                return sum + run.throughput[i];
            }, 0) / runs.length;
        });
        
        overviewTraces.push({
            x: runs[0].time,
            y: avgThroughput,
            type: 'scatter',
            mode: 'lines',
            name: compression,
            line: { color: compressions[compression].color, width: 3 }
        });
    });
    
    Plotly.newPlot('layer_overview', overviewTraces, {
        title: 'Layer 1: Overview (Averages Only)',
        xaxis: { title: 'Time (seconds)' },
        yaxis: { title: 'Throughput (ops/sec)' }
    });
    
    // Layer 2: Add confidence intervals
    var confidenceTraces = [];
    Object.keys(compressions).forEach(function(compression) {
        var runs = getRunsData(compression, entrySize);
        if (runs.length === 0) return;
        
        var avgThroughput = runs[0].time.map(function(_, i) {
            return runs.reduce(function(sum, run) {
                return sum + run.throughput[i];
            }, 0) / runs.length;
        });
        
        var stdThroughput = runs[0].time.map(function(_, i) {
            var avg = avgThroughput[i];
            var variance = runs.reduce(function(sum, run) {
                return sum + Math.pow(run.throughput[i] - avg, 2);
            }, 0) / runs.length;
            return Math.sqrt(variance);
        });
        
        // Upper bound
        confidenceTraces.push({
            x: runs[0].time,
            y: avgThroughput.map(function(val, i) { return val + stdThroughput[i]; }),
            type: 'scatter',
            mode: 'lines',
            name: compression + ' +std',
            line: { color: compressions[compression].color, width: 0 },
            showlegend: false
        });
        
        // Lower bound (fill to upper)
        confidenceTraces.push({
            x: runs[0].time,
            y: avgThroughput.map(function(val, i) { return val - stdThroughput[i]; }),
            type: 'scatter',
            mode: 'lines',
            name: compression,
            line: { color: compressions[compression].color, width: 0 },
            fill: 'tonexty',
            fillcolor: compressions[compression].color.replace(')', ', 0.2)').replace('#', 'rgba(').replace(/(.{2})(.{2})(.{2})/, function(match, r, g, b) {
                return 'rgba(' + parseInt(r, 16) + ',' + parseInt(g, 16) + ',' + parseInt(b, 16) + ', 0.2)';
            })
        });
        
        // Average line
        confidenceTraces.push({
            x: runs[0].time,
            y: avgThroughput,
            type: 'scatter',
            mode: 'lines',
            name: compression,
            line: { color: compressions[compression].color, width: 3 }
        });
    });
    
    Plotly.newPlot('layer_confidence', confidenceTraces, {
        title: 'Layer 2: + Confidence Intervals',
        xaxis: { title: 'Time (seconds)' },
        yaxis: { title: 'Throughput (ops/sec)' }
    });
    
    // Summary ranking
    var scores = {};
    Object.keys(compressions).forEach(function(compression) {
        var runs = getRunsData(compression, entrySize);
        if (runs.length > 0) {
            var throughputScore = runs.reduce(function(sum, run) {
                return sum + run.peak_throughput;
            }, 0) / runs.length;
            var cpuPenalty = runs.reduce(function(sum, run) {
                return sum + run.avg_cpu;
            }, 0) / runs.length;
            var cachePenalty = runs.reduce(function(sum, run) {
                return sum + run.total_cache_misses;
            }, 0) / runs.length / 1000;
            
            scores[compression] = throughputScore - cpuPenalty - cachePenalty;
        }
    });
    
    var sortedScores = Object.keys(scores).sort(function(a, b) {
        return scores[b] - scores[a];
    });
    
    var rankingTrace = {
        x: sortedScores,
        y: sortedScores.map(function(comp) { return scores[comp]; }),
        type: 'bar',
        marker: {
            color: sortedScores.map(function(comp) { return compressions[comp].color; })
        }
    };
    
    Plotly.newPlot('summary_ranking', [rankingTrace], {
        title: 'Performance Ranking',
        xaxis: { title: 'Compression Algorithm' },
        yaxis: { title: 'Overall Score' }
    });
    
    </script>
</body>
</html>
"""
    
    return html_content

def main():
    """Generate all prototype visualizations"""
    print("Generating sample data...")
    data, compressions = generate_sample_data()
    
    print("Creating HTML visualization...")
    html_content = create_html_visualization(data, compressions)
    
    output_file = '/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/visualization_prototypes.html'
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"\nHTML visualization generated: {output_file}")
    print("Open this file in a web browser to see all the prototype approaches!")

if __name__ == "__main__":
    main()