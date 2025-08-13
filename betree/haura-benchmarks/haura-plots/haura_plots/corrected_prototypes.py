#!/usr/bin/env python3
"""
Corrected prototype implementations with matching IDs
"""

import json
import random

def generate_sample_data():
    """Generate realistic sample data mimicking YCSB benchmark results"""
    
    # Compression algorithms and their characteristics
    compressions = {
        'None': {'color': '#7f7f7f', 'cpu_overhead': 0, 'throughput_factor': 1.0},
        'Snappy': {'color': '#2ca02c', 'cpu_overhead': 5, 'throughput_factor': 0.95},
        'Zstd_1': {'color': '#1f77b4', 'cpu_overhead': 15, 'throughput_factor': 0.85},
        'Zstd_5': {'color': '#ff7f0e', 'cpu_overhead': 25, 'throughput_factor': 0.75}
    }
    
    entry_sizes = ['512', '4096', '16384', '30000']
    
    data = {}
    
    for compression, props in compressions.items():
        for entry_size in entry_sizes:
            # Generate 3 runs per configuration
            runs = []
            for run_id in range(3):
                # Time series (60 points for faster rendering)
                time_points = list(range(0, 60, 1))
                
                # Base throughput with some variation
                base_throughput = 1000 + random.gauss(0, 50)
                throughput = base_throughput * props['throughput_factor']
                
                final_throughput = []
                cpu_usage = []
                cache_misses = []
                
                for i, t in enumerate(time_points):
                    # Warmup phase
                    warmup_factor = min(t / 10.0, 1.0)
                    
                    # Throughput with noise
                    noise = random.gauss(0, throughput * 0.05)
                    tp = max(100, throughput * warmup_factor + noise)
                    final_throughput.append(tp)
                    
                    # CPU usage
                    base_cpu = 20 + props['cpu_overhead']
                    cpu = max(5, min(95, base_cpu + random.gauss(0, 3)))
                    cpu_usage.append(cpu)
                    
                    # Cache misses
                    cache = max(0, (1500 - tp/3) + random.gauss(0, 50))
                    cache_misses.append(cache)
                
                runs.append({
                    'run_id': run_id,
                    'time': time_points,
                    'throughput': final_throughput,
                    'cpu_usage': cpu_usage,
                    'cache_misses': cache_misses,
                    'peak_throughput': max(final_throughput),
                    'avg_cpu': sum(cpu_usage) / len(cpu_usage),
                    'total_cache_misses': sum(cache_misses)
                })
            
            data[f"{compression}_{entry_size}"] = runs
    
    return data, compressions

def create_html_visualization(data, compressions):
    """Create HTML visualization with JavaScript charts"""
    
    # Convert data to JSON string
    data_json = json.dumps(data, indent=2)
    compressions_json = json.dumps(compressions, indent=2)
    
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Metrics Visualization Prototypes</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            margin: 20px; 
            background-color: #f8f9fa;
        }}
        .approach {{ 
            margin: 40px 0; 
            border: 2px solid #dee2e6; 
            padding: 25px; 
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .approach h2 {{ 
            color: #495057; 
            border-bottom: 3px solid #007acc; 
            padding-bottom: 10px;
            margin-top: 0;
        }}
        .chart-container {{ 
            margin: 20px 0; 
            min-height: 400px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
        .description {{ 
            background: #e9ecef; 
            padding: 15px; 
            margin: 15px 0; 
            border-radius: 5px;
            border-left: 4px solid #007acc;
        }}
        .grid {{ 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 20px; 
        }}
        .grid-3 {{ 
            display: grid; 
            grid-template-columns: 1fr 1fr 1fr; 
            gap: 20px; 
        }}
        .grid-4 {{ 
            display: grid; 
            grid-template-columns: 1fr 1fr 1fr 1fr; 
            gap: 15px; 
        }}
        .benefits {{ color: #28a745; font-weight: bold; }}
        .use-case {{ color: #6f42c1; font-weight: bold; }}
        .summary {{
            background: #d4edda;
            border: 1px solid #c3e6cb;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }}
        .error {{
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
            color: #721c24;
        }}
        .success {{
            background: #d4edda;
            border: 1px solid #c3e6cb;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
            color: #155724;
        }}
    </style>
</head>
<body>
    <h1>📊 Metrics Visualization Prototypes</h1>
    <div class="summary">
        <h3>Overview</h3>
        <p>These are 6 different approaches to visualize benchmark metrics while preserving individual run details.</p>
        <p><strong>Sample Data:</strong> 4 compression algorithms × 4 entry sizes × 3 runs each = 48 benchmark runs</p>
    </div>

    <div id="status" class="summary">
        <h3>🔄 Loading Visualizations...</h3>
        <p>Please wait while the charts are being generated.</p>
    </div>

    <!-- Approach 1: Small Multiples -->
    <div class="approach">
        <h2>🔍 Approach 1: Small Multiples with Run Overlays</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Direct comparison between compression algorithms, individual run patterns visible<br>
            <div class="use-case">Best for:</div> Time-series data like throughput, CPU usage, memory over time
        </div>
        <div class="grid-4">
            <div id="small_multiple_None" class="chart-container"></div>
            <div id="small_multiple_Snappy" class="chart-container"></div>
            <div id="small_multiple_Zstd_1" class="chart-container"></div>
            <div id="small_multiple_Zstd_5" class="chart-container"></div>
        </div>
    </div>

    <!-- Approach 2: Grouped Lines -->
    <div class="approach">
        <h2>📈 Approach 2: Individual Lines with Group Clustering</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> All algorithms on same timeline, individual variations visible<br>
            <div class="use-case">Best for:</div> Comparing performance patterns across algorithms
        </div>
        <div id="grouped_lines" class="chart-container"></div>
    </div>

    <!-- Approach 3: Box Plots -->
    <div class="approach">
        <h2>📦 Approach 3: Box Plots with Individual Points</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Statistical distribution visible, outliers clearly marked<br>
            <div class="use-case">Best for:</div> Summary statistics like peak performance, total resource usage
        </div>
        <div class="grid-3">
            <div id="boxplot_throughput" class="chart-container"></div>
            <div id="boxplot_cpu" class="chart-container"></div>
            <div id="boxplot_cache" class="chart-container"></div>
        </div>
    </div>

    <!-- Approach 4: Heatmap -->
    <div class="approach">
        <h2>🌡️ Approach 4: Heatmap with Details</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Overview of all configurations, color-coded performance<br>
            <div class="use-case">Best for:</div> Multi-dimensional comparisons across entry sizes and algorithms
        </div>
        <div class="grid-3">
            <div id="heatmap_throughput" class="chart-container"></div>
            <div id="heatmap_cpu" class="chart-container"></div>
            <div id="heatmap_cache" class="chart-container"></div>
        </div>
    </div>

    <!-- Approach 5: Layered -->
    <div class="approach">
        <h2>🎯 Approach 5: Layered Visualization</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Progressive detail disclosure, avoid visual clutter<br>
            <div class="use-case">Best for:</div> Interactive exploration, presentations
        </div>
        <div class="grid">
            <div id="layer_overview" class="chart-container"></div>
            <div id="layer_confidence" class="chart-container"></div>
            <div id="layer_selected" class="chart-container"></div>
            <div id="layer_full" class="chart-container"></div>
        </div>
    </div>

    <!-- Approach 6: Statistical Summary -->
    <div class="approach">
        <h2>📊 Approach 6: Statistical Summary</h2>
        <div class="description">
            <div class="benefits">Benefits:</div> Main trends + detailed breakdowns, performance ranking<br>
            <div class="use-case">Best for:</div> Executive dashboards, comprehensive reports
        </div>
        <div class="grid">
            <div id="summary_main" class="chart-container"></div>
            <div id="summary_ranking" class="chart-container"></div>
        </div>
    </div>

    <script>
    function updateStatus(message, isError = false) {{
        const statusDiv = document.getElementById('status');
        statusDiv.className = isError ? 'error' : 'success';
        statusDiv.innerHTML = '<h3>' + (isError ? '❌ Error' : '✅ Success') + '</h3><p>' + message + '</p>';
    }}

    try {{
        updateStatus('Loading data and initializing charts...');
        
        // Data
        const data = {data_json};
        const compressions = {compressions_json};
        
        console.log('Data loaded:', Object.keys(data).length, 'configurations');
        console.log('Compressions:', Object.keys(compressions));
        
        // Helper function to get runs data
        function getRunsData(compression, entrySize) {{
            const key = compression + '_' + entrySize;
            return data[key] || [];
        }}
        
        // Test data access
        const testRuns = getRunsData('None', '4096');
        console.log('Test data access - None_4096 runs:', testRuns.length);
        
        if (testRuns.length === 0) {{
            throw new Error('No test data found. Data structure might be incorrect.');
        }}
        
        updateStatus('Data loaded successfully. Creating visualizations...');
        
        // Approach 1: Small Multiples
        const entrySize = '4096';
        const compressionList = Object.keys(compressions);
        
        let chartsCreated = 0;
        
        compressionList.forEach(function(compression) {{
            const runs = getRunsData(compression, entrySize);
            console.log('Processing', compression, '- runs:', runs.length);
            
            if (runs.length === 0) {{
                console.warn('No runs found for', compression, entrySize);
                return;
            }}
            
            const traces = [];
            
            // Individual runs (thin lines)
            runs.forEach(function(run, idx) {{
                traces.push({{
                    x: run.time,
                    y: run.throughput,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Run ' + (idx + 1),
                    line: {{ color: compressions[compression].color, width: 1 }},
                    opacity: 0.4,
                    showlegend: idx === 0
                }});
            }});
            
            // Average (thick line)
            const avgThroughput = runs[0].time.map(function(_, i) {{
                return runs.reduce(function(sum, run) {{
                    return sum + run.throughput[i];
                }}, 0) / runs.length;
            }});
            
            traces.push({{
                x: runs[0].time,
                y: avgThroughput,
                type: 'scatter',
                mode: 'lines',
                name: 'Average',
                line: {{ color: compressions[compression].color, width: 3 }}
            }});
            
            const layout = {{
                title: compression + ' - Entry ' + entrySize + 'B',
                xaxis: {{ title: 'Time (seconds)' }},
                yaxis: {{ title: 'Throughput (ops/sec)' }},
                showlegend: true,
                legend: {{ x: 0.7, y: 1 }},
                margin: {{ t: 50, r: 50, b: 50, l: 50 }}
            }};
            
            const divId = 'small_multiple_' + compression;
            const divElement = document.getElementById(divId);
            if (!divElement) {{
                console.error('Div not found:', divId);
                return;
            }}
            
            Plotly.newPlot(divId, traces, layout, {{responsive: true}});
            console.log('Created small multiple for', compression);
            chartsCreated++;
        }});
        
        // Approach 2: Grouped Lines
        const groupedTraces = [];
        Object.keys(compressions).forEach(function(compression) {{
            const runs = getRunsData(compression, entrySize);
            if (runs.length === 0) return;
            
            // Individual runs
            runs.forEach(function(run, idx) {{
                groupedTraces.push({{
                    x: run.time,
                    y: run.throughput,
                    type: 'scatter',
                    mode: 'lines',
                    name: compression + ' Run ' + (idx + 1),
                    line: {{ color: compressions[compression].color, width: 1, dash: 'dot' }},
                    opacity: 0.3,
                    showlegend: false
                }});
            }});
            
            // Average
            const avgThroughput = runs[0].time.map(function(_, i) {{
                return runs.reduce(function(sum, run) {{
                    return sum + run.throughput[i];
                }}, 0) / runs.length;
            }});
            
            groupedTraces.push({{
                x: runs[0].time,
                y: avgThroughput,
                type: 'scatter',
                mode: 'lines',
                name: compression + ' Average',
                line: {{ color: compressions[compression].color, width: 3 }}
            }});
        }});
        
        const groupedLayout = {{
            title: 'Group Clustering: Throughput - Entry ' + entrySize + 'B',
            xaxis: {{ title: 'Time (seconds)' }},
            yaxis: {{ title: 'Throughput (ops/sec)' }},
            legend: {{ x: 0.02, y: 0.98 }},
            margin: {{ t: 50, r: 50, b: 50, l: 50 }}
        }};
        
        Plotly.newPlot('grouped_lines', groupedTraces, groupedLayout, {{responsive: true}});
        console.log('Created grouped lines plot');
        chartsCreated++;
        
        // Approach 3: Box Plots
        const metrics = [
            {{ key: 'peak_throughput', title: 'Peak Throughput (ops/sec)', div: 'boxplot_throughput' }},
            {{ key: 'avg_cpu', title: 'Average CPU Usage (%)', div: 'boxplot_cpu' }},
            {{ key: 'total_cache_misses', title: 'Total Cache Misses', div: 'boxplot_cache' }}
        ];
        
        metrics.forEach(function(metric) {{
            const boxTraces = [];
            Object.keys(compressions).forEach(function(compression) {{
                const runs = getRunsData(compression, entrySize);
                if (runs.length === 0) return;
                
                const values = runs.map(function(run) {{ return run[metric.key]; }});
                
                boxTraces.push({{
                    y: values,
                    type: 'box',
                    name: compression,
                    marker: {{ color: compressions[compression].color }},
                    boxpoints: 'all',
                    pointpos: 0,
                    jitter: 0.3
                }});
            }});
            
            const boxLayout = {{
                title: metric.title + ' Distribution',
                yaxis: {{ title: metric.title }},
                showlegend: false,
                margin: {{ t: 50, r: 50, b: 50, l: 50 }}
            }};
            
            const divElement = document.getElementById(metric.div);
            if (!divElement) {{
                console.error('Div not found:', metric.div);
                return;
            }}
            
            Plotly.newPlot(metric.div, boxTraces, boxLayout, {{responsive: true}});
            console.log('Created box plot for', metric.key);
            chartsCreated++;
        }});
        
        // Approach 4: Heatmaps
        const entrySizes = ['512', '4096', '16384', '30000'];
        const compressionNames = Object.keys(compressions);
        
        metrics.forEach(function(metric) {{
            const zData = [];
            const textData = [];
            
            compressionNames.forEach(function(compression) {{
                const row = [];
                const textRow = [];
                entrySizes.forEach(function(entrySize) {{
                    const runs = getRunsData(compression, entrySize);
                    if (runs.length > 0) {{
                        const values = runs.map(function(run) {{ return run[metric.key]; }});
                        const mean = values.reduce(function(a, b) {{ return a + b; }}) / values.length;
                        const variance = values.reduce(function(sum, val) {{
                            return sum + Math.pow(val - mean, 2);
                        }}, 0) / values.length;
                        const std = Math.sqrt(variance);
                        row.push(mean);
                        textRow.push(mean.toFixed(0) + '±' + std.toFixed(0));
                    }} else {{
                        row.push(0);
                        textRow.push('N/A');
                    }}
                }});
                zData.push(row);
                textData.push(textRow);
            }});
            
            const heatmapTrace = {{
                z: zData,
                text: textData,
                texttemplate: '%{{text}}',
                textfont: {{ color: 'white', size: 10 }},
                type: 'heatmap',
                colorscale: 'Viridis',
                x: entrySizes,
                y: compressionNames,
                hoverongaps: false
            }};
            
            const heatmapLayout = {{
                title: metric.title + ' Heatmap (mean ± std)',
                xaxis: {{ title: 'Entry Size (bytes)' }},
                yaxis: {{ title: 'Compression Algorithm' }},
                margin: {{ t: 50, r: 50, b: 50, l: 50 }}
            }};
            
            // Map metric key to div ID correctly
            let heatmapDiv;
            if (metric.key === 'peak_throughput') {{
                heatmapDiv = 'heatmap_throughput';
            }} else if (metric.key === 'avg_cpu') {{
                heatmapDiv = 'heatmap_cpu';
            }} else if (metric.key === 'total_cache_misses') {{
                heatmapDiv = 'heatmap_cache';
            }}
            
            const divElement = document.getElementById(heatmapDiv);
            if (!divElement) {{
                console.error('Div not found:', heatmapDiv);
                return;
            }}
            
            Plotly.newPlot(heatmapDiv, [heatmapTrace], heatmapLayout, {{responsive: true}});
            console.log('Created heatmap for', metric.key, 'in div', heatmapDiv);
            chartsCreated++;
        }});
        
        // Approach 5: Layered Visualization
        
        // Layer 1: Overview (averages only)
        const overviewTraces = [];
        Object.keys(compressions).forEach(function(compression) {{
            const runs = getRunsData(compression, entrySize);
            if (runs.length === 0) return;
            
            const avgThroughput = runs[0].time.map(function(_, i) {{
                return runs.reduce(function(sum, run) {{
                    return sum + run.throughput[i];
                }}, 0) / runs.length;
            }});
            
            overviewTraces.push({{
                x: runs[0].time,
                y: avgThroughput,
                type: 'scatter',
                mode: 'lines',
                name: compression,
                line: {{ color: compressions[compression].color, width: 3 }}
            }});
        }});
        
        Plotly.newPlot('layer_overview', overviewTraces, {{
            title: 'Layer 1: Overview (Averages Only)',
            xaxis: {{ title: 'Time (seconds)' }},
            yaxis: {{ title: 'Throughput (ops/sec)' }},
            margin: {{ t: 50, r: 50, b: 50, l: 50 }}
        }}, {{responsive: true}});
        chartsCreated++;
        
        // Layer 2: Add confidence intervals
        const confidenceTraces = [];
        Object.keys(compressions).forEach(function(compression) {{
            const runs = getRunsData(compression, entrySize);
            if (runs.length === 0) return;
            
            const avgThroughput = runs[0].time.map(function(_, i) {{
                return runs.reduce(function(sum, run) {{
                    return sum + run.throughput[i];
                }}, 0) / runs.length;
            }});
            
            const stdThroughput = runs[0].time.map(function(_, i) {{
                const avg = avgThroughput[i];
                const variance = runs.reduce(function(sum, run) {{
                    return sum + Math.pow(run.throughput[i] - avg, 2);
                }}, 0) / runs.length;
                return Math.sqrt(variance);
            }});
            
            // Confidence interval
            const upperBound = avgThroughput.map(function(val, i) {{ return val + stdThroughput[i]; }});
            const lowerBound = avgThroughput.map(function(val, i) {{ return val - stdThroughput[i]; }});
            
            // Fill area
            confidenceTraces.push({{
                x: runs[0].time.concat(runs[0].time.slice().reverse()),
                y: upperBound.concat(lowerBound.slice().reverse()),
                fill: 'toself',
                fillcolor: compressions[compression].color.replace('#', 'rgba(').replace(/(.{{2}})(.{{2}})(.{{2}})/, function(match, r, g, b) {{
                    return 'rgba(' + parseInt(r, 16) + ',' + parseInt(g, 16) + ',' + parseInt(b, 16) + ', 0.2)';
                }}),
                line: {{ color: 'transparent' }},
                name: compression + ' ±1σ',
                showlegend: false
            }});
            
            // Average line
            confidenceTraces.push({{
                x: runs[0].time,
                y: avgThroughput,
                type: 'scatter',
                mode: 'lines',
                name: compression,
                line: {{ color: compressions[compression].color, width: 3 }}
            }});
        }});
        
        Plotly.newPlot('layer_confidence', confidenceTraces, {{
            title: 'Layer 2: + Confidence Intervals (±1σ)',
            xaxis: {{ title: 'Time (seconds)' }},
            yaxis: {{ title: 'Throughput (ops/sec)' }},
            margin: {{ t: 50, r: 50, b: 50, l: 50 }}
        }}, {{responsive: true}});
        chartsCreated++;
        
        // Layer 3: Selected algorithms with individual runs
        const selectedTraces = [];
        const selectedAlgos = ['None', 'Snappy', 'Zstd_1'];
        
        selectedAlgos.forEach(function(compression) {{
            if (!compressions[compression]) return;
            const runs = getRunsData(compression, entrySize);
            if (runs.length === 0) return;
            
            // Individual runs
            runs.forEach(function(run, idx) {{
                selectedTraces.push({{
                    x: run.time,
                    y: run.throughput,
                    type: 'scatter',
                    mode: 'lines',
                    name: compression + ' Run ' + (idx + 1),
                    line: {{ color: compressions[compression].color, width: 1 }},
                    opacity: 0.4,
                    showlegend: idx === 0
                }});
            }});
            
            // Average
            const avgThroughput = runs[0].time.map(function(_, i) {{
                return runs.reduce(function(sum, run) {{
                    return sum + run.throughput[i];
                }}, 0) / runs.length;
            }});
            
            selectedTraces.push({{
                x: runs[0].time,
                y: avgThroughput,
                type: 'scatter',
                mode: 'lines',
                name: compression + ' Average',
                line: {{ color: compressions[compression].color, width: 3 }}
            }});
        }});
        
        Plotly.newPlot('layer_selected', selectedTraces, {{
            title: 'Layer 3: + Individual Runs (Selected Algorithms)',
            xaxis: {{ title: 'Time (seconds)' }},
            yaxis: {{ title: 'Throughput (ops/sec)' }},
            margin: {{ t: 50, r: 50, b: 50, l: 50 }}
        }}, {{responsive: true}});
        chartsCreated++;
        
        // Layer 4: Full detail (all algorithms, all runs)
        const fullTraces = [];
        Object.keys(compressions).forEach(function(compression) {{
            const runs = getRunsData(compression, entrySize);
            if (runs.length === 0) return;
            
            // Individual runs
            runs.forEach(function(run, idx) {{
                fullTraces.push({{
                    x: run.time,
                    y: run.throughput,
                    type: 'scatter',
                    mode: 'lines',
                    name: compression + ' Run ' + (idx + 1),
                    line: {{ color: compressions[compression].color, width: 1 }},
                    opacity: 0.3,
                    showlegend: false
                }});
            }});
            
            // Average
            const avgThroughput = runs[0].time.map(function(_, i) {{
                return runs.reduce(function(sum, run) {{
                    return sum + run.throughput[i];
                }}, 0) / runs.length;
            }});
            
            fullTraces.push({{
                x: runs[0].time,
                y: avgThroughput,
                type: 'scatter',
                mode: 'lines',
                name: compression + ' Average',
                line: {{ color: compressions[compression].color, width: 3 }}
            }});
        }});
        
        Plotly.newPlot('layer_full', fullTraces, {{
            title: 'Layer 4: Full Detail (All Algorithms, All Runs)',
            xaxis: {{ title: 'Time (seconds)' }},
            yaxis: {{ title: 'Throughput (ops/sec)' }},
            margin: {{ t: 50, r: 50, b: 50, l: 50 }}
        }}, {{responsive: true}});
        chartsCreated++;
        
        // Approach 6: Statistical Summary
        
        // Main plot with confidence intervals (reuse from layer 2)
        Plotly.newPlot('summary_main', confidenceTraces, {{
            title: 'Main Plot: Group Averages ± Standard Deviation',
            xaxis: {{ title: 'Time (seconds)' }},
            yaxis: {{ title: 'Throughput (ops/sec)' }},
            margin: {{ t: 50, r: 50, b: 50, l: 50 }}
        }}, {{responsive: true}});
        chartsCreated++;
        
        // Performance ranking
        const scores = {{}};
        Object.keys(compressions).forEach(function(compression) {{
            const runs = getRunsData(compression, entrySize);
            if (runs.length > 0) {{
                const throughputScore = runs.reduce(function(sum, run) {{
                    return sum + run.peak_throughput;
                }}, 0) / runs.length;
                const cpuPenalty = runs.reduce(function(sum, run) {{
                    return sum + run.avg_cpu;
                }}, 0) / runs.length;
                const cachePenalty = runs.reduce(function(sum, run) {{
                    return sum + run.total_cache_misses;
                }}, 0) / runs.length / 1000;
                
                // Overall score: higher throughput is better, lower CPU and cache misses are better
                scores[compression] = throughputScore - cpuPenalty - cachePenalty;
            }}
        }});
        
        const sortedScores = Object.keys(scores).sort(function(a, b) {{
            return scores[b] - scores[a];
        }});
        
        const rankingTrace = {{
            x: sortedScores,
            y: sortedScores.map(function(comp) {{ return scores[comp]; }}),
            type: 'bar',
            marker: {{
                color: sortedScores.map(function(comp) {{ return compressions[comp].color; }})
            }},
            text: sortedScores.map(function(comp) {{ return scores[comp].toFixed(0); }}),
            textposition: 'auto'
        }};
        
        Plotly.newPlot('summary_ranking', [rankingTrace], {{
            title: 'Performance Ranking (Throughput - CPU - Cache/1000)',
            xaxis: {{ title: 'Compression Algorithm' }},
            yaxis: {{ title: 'Overall Score' }},
            margin: {{ t: 50, r: 50, b: 50, l: 50 }}
        }}, {{responsive: true}});
        chartsCreated++;
        
        updateStatus(`All visualizations loaded successfully! Created ${{chartsCreated}} charts.`);
        console.log('All visualizations loaded successfully! Total charts:', chartsCreated);
        
    }} catch (error) {{
        console.error('Error creating visualizations:', error);
        updateStatus('Error creating visualizations: ' + error.message + '. Check browser console for details.', true);
    }}
    </script>
</body>
</html>"""
    
    return html_content

def main():
    """Generate all prototype visualizations"""
    print("Generating sample data...")
    data, compressions = generate_sample_data()
    
    print("Creating HTML visualization...")
    html_content = create_html_visualization(data, compressions)
    
    output_file = '/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/corrected_visualization_prototypes.html'
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"\n✅ Corrected HTML visualization generated: {output_file}")
    print("🌐 Open this file in a web browser to see all the prototype approaches!")
    print("\n🔧 This version fixes:")
    print("- Matching div IDs between HTML and JavaScript")
    print("- Better error handling with specific error messages")
    print("- Status updates showing progress")
    print("- Proper element existence checking")

if __name__ == "__main__":
    main()