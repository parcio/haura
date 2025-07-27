#!/usr/bin/env python3
"""
Basic prototype implementations without any complex dependencies
"""

import json
import random
import math

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
                time_points = list(range(0, 300, 1))
                
                # Base throughput with some variation
                base_throughput = 1000 + random.gauss(0, 50)
                throughput = base_throughput * props['throughput_factor']
                
                final_throughput = []
                cpu_usage = []
                memory_usage = []
                tier_usage = []
                cache_misses = []
                
                cumulative_tier = 1000
                
                for i, t in enumerate(time_points):
                    # Warmup phase
                    warmup_factor = min(t / 30.0, 1.0)
                    
                    # Throughput with noise and occasional spikes
                    noise = random.gauss(0, throughput * 0.1)
                    spike = random.gauss(0, throughput * 0.3) if random.random() < 0.05 else 0
                    
                    tp = max(0, throughput * warmup_factor + noise + spike)
                    final_throughput.append(tp)
                    
                    # CPU usage
                    base_cpu = 20 + props['cpu_overhead']
                    cpu = max(0, min(100, base_cpu + random.gauss(0, 5)))
                    cpu_usage.append(cpu)
                    
                    # Memory usage (gradually increasing)
                    memory_base = 500 + int(entry_size) * 0.1
                    memory = memory_base + t * 0.5 + random.gauss(0, 20)
                    memory_usage.append(memory)
                    
                    # Storage tier usage
                    cumulative_tier += random.expovariate(0.5)
                    tier_usage.append(cumulative_tier)
                    
                    # Cache misses (inversely related to throughput)
                    cache = max(0, (2000 - tp/2) + random.gauss(0, 100))
                    cache_misses.append(cache)
                
                runs.append({
                    'run_id': run_id,
                    'time': time_points,
                    'throughput': final_throughput,
                    'cpu_usage': cpu_usage,
                    'memory_usage': memory_usage,
                    'tier_usage': tier_usage,
                    'cache_misses': cache_misses,
                    'peak_throughput': max(final_throughput),
                    'avg_cpu': sum(cpu_usage) / len(cpu_usage),
                    'total_cache_misses': sum(cache_misses)
                })
            
            data[f"{compression},{entry_size}"] = runs
    
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
        body { 
            font-family: Arial, sans-serif; 
            margin: 20px; 
            background-color: #f8f9fa;
        }
        .approach { 
            margin: 40px 0; 
            border: 2px solid #dee2e6; 
            padding: 25px; 
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .approach h2 { 
            color: #495057; 
            border-bottom: 3px solid #007acc; 
            padding-bottom: 10px;
            margin-top: 0;
        }
        .chart-container { 
            margin: 20px 0; 
            min-height: 400px;
        }
        .description { 
            background: #e9ecef; 
            padding: 15px; 
            margin: 15px 0; 
            border-radius: 5px;
            border-left: 4px solid #007acc;
        }
        .grid { 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 20px; 
        }
        .grid-3 { 
            display: grid; 
            grid-template-columns: 1fr 1fr 1fr; 
            gap: 20px; 
        }
        .grid-4 { 
            display: grid; 
            grid-template-columns: 1fr 1fr 1fr 1fr; 
            gap: 15px; 
        }
        .benefits { color: #28a745; font-weight: bold; }
        .use-case { color: #6f42c1; font-weight: bold; }
        .summary {
            background: #d4edda;
            border: 1px solid #c3e6cb;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }
        .recommendation {
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
        }
    </style>
</head>
<body>
    <h1>📊 Metrics Visualization Prototypes</h1>
    <div class="summary">
        <h3>Overview</h3>
        <p>These are 6 different approaches to visualize benchmark metrics while preserving individual run details. 
        Each approach has different strengths for different types of analysis.</p>
        <p><strong>Sample Data:</strong> 5 compression algorithms × 4 entry sizes × 3 runs each = 60 benchmark runs</p>
    </div>
"""
    
    # Approach 1: Small Multiples
    html_content += """
    <div class="approach">
        <h2>🔍 Approach 1: Small Multiples with Run Overlays</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Direct comparison between compression algorithms, individual run patterns visible, easy to spot outliers and consistency<br>
            <div class="use-case">Best for:</div> Time-series data like throughput, CPU usage, memory over time<br>
            <div class="recommendation">💡 <strong>Recommendation:</strong> Excellent for seeing both individual run behavior AND cross-algorithm comparison</div>
        </div>
        <div class="grid-4">
"""
    
    entry_size = '4096'
    for compression in list(compressions.keys())[:4]:  # Show first 4
        runs = data.get((compression, entry_size), [])
        if runs:
            html_content += f'            <div id="small_multiple_{compression.replace("(", "_").replace(")", "_")}" class="chart-container"></div>\n'
    
    html_content += """        </div>
    </div>
"""
    
    # Approach 2: Grouped Lines
    html_content += """
    <div class="approach">
        <h2>📈 Approach 2: Individual Lines with Group Clustering</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> All algorithms on same timeline, individual variations visible, group averages clear, easy to compare patterns<br>
            <div class="use-case">Best for:</div> Comparing performance patterns across algorithms on same time scale<br>
            <div class="recommendation">💡 <strong>Recommendation:</strong> Great for seeing relative performance and timing differences</div>
        </div>
        <div id="grouped_lines" class="chart-container"></div>
    </div>
"""
    
    # Approach 3: Box Plots
    html_content += """
    <div class="approach">
        <h2>📦 Approach 3: Box Plots with Individual Points</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Statistical distribution visible, outliers clearly marked, easy comparison of variability<br>
            <div class="use-case">Best for:</div> Summary statistics like peak performance, total resource usage, comparing consistency<br>
            <div class="recommendation">💡 <strong>Recommendation:</strong> Perfect for executive summaries and statistical analysis</div>
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
        <h2>🌡️ Approach 4: Heatmap with Details</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Overview of all configurations at once, color-coded performance, compact representation<br>
            <div class="use-case">Best for:</div> Multi-dimensional comparisons across entry sizes and algorithms, finding optimal configurations<br>
            <div class="recommendation">💡 <strong>Recommendation:</strong> Excellent for parameter space exploration and optimization</div>
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
        <h2>🎯 Approach 5: Layered Visualization (Progressive Detail)</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Start simple, add detail on demand, avoid visual clutter, progressive disclosure<br>
            <div class="use-case">Best for:</div> Interactive exploration, presentations where you want to build up complexity<br>
            <div class="recommendation">💡 <strong>Recommendation:</strong> Great for storytelling and guided analysis</div>
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
        <h2>📊 Approach 6: Statistical Summary with Details</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Main trends + detailed breakdowns, performance ranking, comprehensive overview<br>
            <div class="use-case">Best for:</div> Executive dashboards, comprehensive reports, decision making<br>
            <div class="recommendation">💡 <strong>Recommendation:</strong> Perfect for final reports and decision support</div>
        </div>
        <div class="grid">
            <div id="summary_main" class="chart-container"></div>
            <div id="summary_ranking" class="chart-container"></div>
        </div>
    </div>
"""
    
    # Final recommendations
    html_content += """
    <div class="summary">
        <h3>🎯 Final Recommendations</h3>
        <p><strong>For metrics_plots.py implementation:</strong></p>
        <ul>
            <li><strong>Time-series metrics</strong> (throughput, CPU, memory): Use <strong>Approach 1 (Small Multiples)</strong> or <strong>Approach 2 (Grouped Lines)</strong></li>
            <li><strong>Summary statistics</strong> (peak values, totals): Use <strong>Approach 3 (Box Plots)</strong></li>
            <li><strong>Multi-dimensional analysis</strong>: Use <strong>Approach 4 (Heatmaps)</strong></li>
            <li><strong>Interactive exploration</strong>: Use <strong>Approach 5 (Layered)</strong></li>
            <li><strong>Executive reports</strong>: Use <strong>Approach 6 (Statistical Summary)</strong></li>
        </ul>
        <p><strong>Hybrid approach:</strong> Combine multiple approaches in a single dashboard for comprehensive analysis.</p>
    </div>
"""
    
    # JavaScript for charts
    html_content += """
    <script>
    // Data conversion helper
    function convertDataKey(compression, entrySize) {
        // Find matching key in data object
        for (var key in data) {
            if (key.includes(compression) && key.includes(entrySize)) {
                return key;
            }
        }
        return null;
    }
    
    // Data
    var data = """ + json.dumps(data, default=str) + """;
    var compressions = """ + json.dumps(compressions) + """;
    
    // Helper function to get runs data
    function getRunsData(compression, entrySize) {
        var key = convertDataKey(compression, entrySize);
        return key ? data[key] : [];
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
                opacity: 0.4,
                showlegend: idx === 0
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
            showlegend: true,
            legend: { x: 0.7, y: 1 }
        };
        
        var divId = 'small_multiple_' + compression.replace(/[()]/g, '_');
        Plotly.newPlot(divId, traces, layout, {responsive: true});
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
                showlegend: false
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
        yaxis: { title: 'Throughput (ops/sec)' },
        legend: { x: 0.02, y: 0.98 }
    };
    
    Plotly.newPlot('grouped_lines', groupedTraces, groupedLayout, {responsive: true});
    
    // Approach 3: Box Plots
    var metrics = [
        { key: 'peak_throughput', title: 'Peak Throughput (ops/sec)', div: 'boxplot_throughput' },
        { key: 'avg_cpu', title: 'Average CPU Usage (%)', div: 'boxplot_cpu' },
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
                marker: { color: compressions[compression].color },
                boxpoints: 'all',
                pointpos: 0,
                jitter: 0.3
            });
        });
        
        var boxLayout = {
            title: metric.title + ' Distribution',
            yaxis: { title: metric.title },
            showlegend: false
        };
        
        Plotly.newPlot(metric.div, boxTraces, boxLayout, {responsive: true});
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
                    var variance = values.reduce(function(sum, val) {
                        return sum + Math.pow(val - mean, 2);
                    }, 0) / values.length;
                    var std = Math.sqrt(variance);
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
            textfont: { color: 'white', size: 10 },
            type: 'heatmap',
            colorscale: 'Viridis',
            x: entrySizes,
            y: compressionNames,
            hoverongaps: false
        };
        
        var heatmapLayout = {
            title: metric.title + ' Heatmap (mean ± std)',
            xaxis: { title: 'Entry Size (bytes)' },
            yaxis: { title: 'Compression Algorithm' }
        };
        
        var heatmapDiv = 'heatmap_' + metric.key.split('_').pop();
        Plotly.newPlot(heatmapDiv, [heatmapTrace], heatmapLayout, {responsive: true});
    });
    
    // Approach 5: Layered Visualization
    
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
    }, {responsive: true});
    
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
        
        // Confidence interval
        var upperBound = avgThroughput.map(function(val, i) { return val + stdThroughput[i]; });
        var lowerBound = avgThroughput.map(function(val, i) { return val - stdThroughput[i]; });
        
        // Fill area
        confidenceTraces.push({
            x: runs[0].time.concat(runs[0].time.slice().reverse()),
            y: upperBound.concat(lowerBound.slice().reverse()),
            fill: 'toself',
            fillcolor: compressions[compression].color.replace('#', 'rgba(').replace(/(.{2})(.{2})(.{2})/, function(match, r, g, b) {
                return 'rgba(' + parseInt(r, 16) + ',' + parseInt(g, 16) + ',' + parseInt(b, 16) + ', 0.2)';
            }),
            line: { color: 'transparent' },
            name: compression + ' ±1σ',
            showlegend: false
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
        title: 'Layer 2: + Confidence Intervals (±1σ)',
        xaxis: { title: 'Time (seconds)' },
        yaxis: { title: 'Throughput (ops/sec)' }
    }, {responsive: true});
    
    // Layer 3: Selected algorithms with individual runs
    var selectedTraces = [];
    var selectedAlgos = ['None', 'Snappy', 'Zstd(1)'];
    
    selectedAlgos.forEach(function(compression) {
        if (!compressions[compression]) return;
        var runs = getRunsData(compression, entrySize);
        if (runs.length === 0) return;
        
        // Individual runs
        runs.forEach(function(run, idx) {
            selectedTraces.push({
                x: run.time,
                y: run.throughput,
                type: 'scatter',
                mode: 'lines',
                name: compression + ' Run ' + (idx + 1),
                line: { color: compressions[compression].color, width: 1 },
                opacity: 0.4,
                showlegend: idx === 0
            });
        });
        
        // Average
        var avgThroughput = runs[0].time.map(function(_, i) {
            return runs.reduce(function(sum, run) {
                return sum + run.throughput[i];
            }, 0) / runs.length;
        });
        
        selectedTraces.push({
            x: runs[0].time,
            y: avgThroughput,
            type: 'scatter',
            mode: 'lines',
            name: compression + ' Average',
            line: { color: compressions[compression].color, width: 3 }
        });
    });
    
    Plotly.newPlot('layer_selected', selectedTraces, {
        title: 'Layer 3: + Individual Runs (Selected Algorithms)',
        xaxis: { title: 'Time (seconds)' },
        yaxis: { title: 'Throughput (ops/sec)' }
    }, {responsive: true});
    
    // Layer 4: Full detail (all algorithms, all runs)
    var fullTraces = [];
    Object.keys(compressions).forEach(function(compression) {
        var runs = getRunsData(compression, entrySize);
        if (runs.length === 0) return;
        
        // Individual runs
        runs.forEach(function(run, idx) {
            fullTraces.push({
                x: run.time,
                y: run.throughput,
                type: 'scatter',
                mode: 'lines',
                name: compression + ' Run ' + (idx + 1),
                line: { color: compressions[compression].color, width: 1 },
                opacity: 0.3,
                showlegend: false
            });
        });
        
        // Average
        var avgThroughput = runs[0].time.map(function(_, i) {
            return runs.reduce(function(sum, run) {
                return sum + run.throughput[i];
            }, 0) / runs.length;
        });
        
        fullTraces.push({
            x: runs[0].time,
            y: avgThroughput,
            type: 'scatter',
            mode: 'lines',
            name: compression + ' Average',
            line: { color: compressions[compression].color, width: 3 }
        });
    });
    
    Plotly.newPlot('layer_full', fullTraces, {
        title: 'Layer 4: Full Detail (All Algorithms, All Runs)',
        xaxis: { title: 'Time (seconds)' },
        yaxis: { title: 'Throughput (ops/sec)' }
    }, {responsive: true});
    
    // Approach 6: Statistical Summary
    
    // Main plot with confidence intervals (reuse from layer 2)
    Plotly.newPlot('summary_main', confidenceTraces, {
        title: 'Main Plot: Group Averages ± Standard Deviation',
        xaxis: { title: 'Time (seconds)' },
        yaxis: { title: 'Throughput (ops/sec)' }
    }, {responsive: true});
    
    // Performance ranking
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
            
            // Overall score: higher throughput is better, lower CPU and cache misses are better
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
        },
        text: sortedScores.map(function(comp) { return scores[comp].toFixed(0); }),
        textposition: 'auto'
    };
    
    Plotly.newPlot('summary_ranking', [rankingTrace], {
        title: 'Performance Ranking (Throughput - CPU - Cache/1000)',
        xaxis: { title: 'Compression Algorithm' },
        yaxis: { title: 'Overall Score' }
    }, {responsive: true});
    
    console.log('All visualizations loaded successfully!');
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
    
    print(f"\n✅ HTML visualization generated: {output_file}")
    print("🌐 Open this file in a web browser to see all the prototype approaches!")
    print("\n📋 Summary of approaches:")
    print("1. 🔍 Small Multiples - Side-by-side comparison with individual runs")
    print("2. 📈 Grouped Lines - All algorithms on same timeline")
    print("3. 📦 Box Plots - Statistical distributions with outliers")
    print("4. 🌡️ Heatmaps - Multi-dimensional overview")
    print("5. 🎯 Layered - Progressive detail disclosure")
    print("6. 📊 Statistical Summary - Executive dashboard style")

if __name__ == "__main__":
    main()