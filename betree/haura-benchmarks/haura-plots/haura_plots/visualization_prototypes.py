#!/usr/bin/env python3
"""
Prototype implementations of different visualization approaches for metrics_plots.py
This generates sample plots to help decide which approach works best.
"""

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
try:
    import seaborn as sns
    sns.set_palette("husl")
except ImportError:
    print("Seaborn not available, using matplotlib defaults")
    sns = None
from matplotlib.patches import Rectangle
try:
    import pandas as pd
except ImportError:
    print("Pandas not available, using basic data structures")
    pd = None
from datetime import datetime, timedelta

# Set style
plt.style.use('default')

# Sample data generation to mimic your benchmark results
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
                    'time': time_points,
                    'throughput': final_throughput,
                    'cpu_usage': cpu_usage,
                    'memory_usage': memory_usage,
                    'tier_usage': tier_usage,
                    'cache_misses': cache_misses,
                    'peak_throughput': np.max(final_throughput),
                    'avg_cpu': np.mean(cpu_usage),
                    'total_cache_misses': np.sum(cache_misses)
                })
            
            data[(compression, entry_size)] = runs
    
    return data, compressions

# Approach 1: Small Multiples with Run Overlays
def plot_small_multiples_with_overlays(data, compressions, metric='throughput'):
    """Small multiples showing each compression type with individual runs overlaid"""
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.flatten()
    
    entry_size = '4096'  # Focus on one entry size for clarity
    compression_list = list(compressions.keys())
    
    for idx, compression in enumerate(compression_list):
        if idx >= len(axes):
            break
            
        ax = axes[idx]
        runs = data.get((compression, entry_size), [])
        
        if not runs:
            continue
            
        # Plot individual runs (thin, semi-transparent)
        for run in runs:
            ax.plot(run['time'], run[metric], 
                   color=compressions[compression]['color'], 
                   alpha=0.4, linewidth=1, 
                   label=f'Run {run["run_id"]+1}' if run['run_id'] == 0 else "")
        
        # Calculate and plot average (thick, prominent)
        if runs:
            avg_data = np.mean([run[metric] for run in runs], axis=0)
            ax.plot(runs[0]['time'], avg_data, 
                   color=compressions[compression]['color'], 
                   linewidth=3, label='Average')
        
        ax.set_title(f'{compression} - Entry {entry_size}B', fontsize=12, fontweight='bold')
        ax.set_xlabel('Time (seconds)')
        ax.set_ylabel('Throughput (ops/sec)' if metric == 'throughput' else metric.replace('_', ' ').title())
        ax.grid(True, alpha=0.3)
        
        if idx == 0:  # Only show legend on first subplot
            ax.legend(loc='upper right', fontsize=8)
    
    # Remove empty subplots
    for idx in range(len(compression_list), len(axes)):
        fig.delaxes(axes[idx])
    
    plt.tight_layout()
    plt.suptitle(f'Small Multiples: {metric.replace("_", " ").title()} by Compression Algorithm', 
                 fontsize=16, y=0.98)
    plt.savefig(f'/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/prototype_small_multiples_{metric}.png', 
                dpi=150, bbox_inches='tight')
    plt.close()

# Approach 2: Individual Lines with Group Clustering
def plot_grouped_lines(data, compressions, metric='throughput'):
    """All compression types on same plot with individual runs clustered by type"""
    
    fig, ax = plt.subplots(1, 1, figsize=(15, 8))
    
    entry_size = '4096'
    
    for compression, props in compressions.items():
        runs = data.get((compression, entry_size), [])
        
        if not runs:
            continue
        
        # Plot individual runs
        for i, run in enumerate(runs):
            label = f'{compression} Run {i+1}' if i == 0 else ""
            ax.plot(run['time'], run[metric], 
                   color=props['color'], alpha=0.3, linewidth=1,
                   linestyle='--', label=label)
        
        # Plot average
        if runs:
            avg_data = np.mean([run[metric] for run in runs], axis=0)
            ax.plot(runs[0]['time'], avg_data, 
                   color=props['color'], linewidth=3, 
                   label=f'{compression} Average')
    
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('Throughput (ops/sec)' if metric == 'throughput' else metric.replace('_', ' ').title())
    ax.set_title(f'Group Clustering: {metric.replace("_", " ").title()} - Entry {entry_size}B')
    ax.grid(True, alpha=0.3)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    plt.tight_layout()
    plt.savefig(f'/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/prototype_grouped_lines_{metric}.png', 
                dpi=150, bbox_inches='tight')
    plt.close()

# Approach 3: Box Plots with Individual Points
def plot_boxplots_with_points(data, compressions):
    """Box plots showing distribution with individual run points"""
    
    # Prepare data for different metrics
    metrics = ['peak_throughput', 'avg_cpu', 'total_cache_misses']
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    
    entry_size = '4096'
    
    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        
        # Collect data for box plot
        plot_data = []
        labels = []
        colors = []
        
        for compression, props in compressions.items():
            runs = data.get((compression, entry_size), [])
            if runs:
                values = [run[metric] for run in runs]
                plot_data.extend(values)
                labels.extend([compression] * len(values))
                colors.extend([props['color']] * len(values))
        
        # Create data structure (with or without pandas)
        if pd is not None:
            df = pd.DataFrame({
                'Compression': labels,
                'Value': plot_data,
                'Color': colors
            })
        else:
            # Simple dictionary structure
            df = {
                'Compression': labels,
                'Value': plot_data,
                'Color': colors
            }
        
        # Prepare data for box plot
        box_data = []
        for compression in compressions.keys():
            if pd is not None:
                comp_values = df[df['Compression'] == compression]['Value'].values
            else:
                # Manual filtering
                comp_values = [df['Value'][i] for i, comp in enumerate(df['Compression']) 
                              if comp == compression]
            box_data.append(comp_values)
        
        # Box plot
        box_plot = ax.boxplot(box_data,
                             labels=list(compressions.keys()),
                             patch_artist=True)
        
        # Color the boxes
        for patch, compression in zip(box_plot['boxes'], compressions.keys()):
            patch.set_facecolor(compressions[compression]['color'])
            patch.set_alpha(0.7)
        
        # Add individual points
        for i, compression in enumerate(compressions.keys()):
            comp_data = box_data[i]
            x_pos = np.random.normal(i+1, 0.04, len(comp_data))
            ax.scatter(x_pos, comp_data, 
                      color=compressions[compression]['color'], 
                      alpha=0.8, s=30, zorder=3)
        
        ax.set_title(f'{metric.replace("_", " ").title()}')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.suptitle(f'Box Plots with Individual Points - Entry {entry_size}B', 
                 fontsize=16, y=1.02)
    plt.savefig('/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/prototype_boxplots.png', 
                dpi=150, bbox_inches='tight')
    plt.close()

# Approach 4: Heatmap with Drill-down Capability
def plot_heatmap_with_details(data, compressions):
    """Heatmap showing performance across configurations"""
    
    entry_sizes = ['512', '4096', '16384', '30000']
    compression_names = list(compressions.keys())
    
    # Create matrices for different metrics
    metrics = ['peak_throughput', 'avg_cpu', 'total_cache_misses']
    
    fig, axes = plt.subplots(1, 3, figsize=(20, 6))
    
    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        
        # Create matrix
        matrix = np.zeros((len(compression_names), len(entry_sizes)))
        std_matrix = np.zeros((len(compression_names), len(entry_sizes)))
        
        for i, compression in enumerate(compression_names):
            for j, entry_size in enumerate(entry_sizes):
                runs = data.get((compression, entry_size), [])
                if runs:
                    values = [run[metric] for run in runs]
                    matrix[i, j] = np.mean(values)
                    std_matrix[i, j] = np.std(values)
        
        # Create heatmap
        im = ax.imshow(matrix, cmap='viridis', aspect='auto')
        
        # Add text annotations with mean ± std
        for i in range(len(compression_names)):
            for j in range(len(entry_sizes)):
                if matrix[i, j] > 0:
                    text = f'{matrix[i, j]:.0f}\n±{std_matrix[i, j]:.0f}'
                    ax.text(j, i, text, ha="center", va="center", 
                           color="white" if matrix[i, j] > matrix.max()/2 else "black",
                           fontsize=8)
        
        ax.set_xticks(range(len(entry_sizes)))
        ax.set_xticklabels(entry_sizes)
        ax.set_yticks(range(len(compression_names)))
        ax.set_yticklabels(compression_names)
        ax.set_xlabel('Entry Size (bytes)')
        ax.set_ylabel('Compression Algorithm')
        ax.set_title(f'{metric.replace("_", " ").title()}')
        
        # Add colorbar
        plt.colorbar(im, ax=ax, shrink=0.8)
    
    plt.tight_layout()
    plt.suptitle('Heatmap: Performance Overview (mean ± std)', fontsize=16, y=1.02)
    plt.savefig('/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/prototype_heatmap.png', 
                dpi=150, bbox_inches='tight')
    plt.close()

# Approach 5: Interactive Layered Plots (Static version)
def plot_layered_visualization(data, compressions, metric='throughput'):
    """Layered plot showing progressive detail disclosure"""
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    entry_size = '4096'
    
    # Layer 1: Overview - Just averages
    ax1 = axes[0, 0]
    for compression, props in compressions.items():
        runs = data.get((compression, entry_size), [])
        if runs:
            avg_data = np.mean([run[metric] for run in runs], axis=0)
            ax1.plot(runs[0]['time'], avg_data, 
                    color=props['color'], linewidth=3, 
                    label=compression)
    ax1.set_title('Layer 1: Overview (Averages Only)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Layer 2: Add confidence intervals
    ax2 = axes[0, 1]
    for compression, props in compressions.items():
        runs = data.get((compression, entry_size), [])
        if runs:
            data_matrix = np.array([run[metric] for run in runs])
            avg_data = np.mean(data_matrix, axis=0)
            std_data = np.std(data_matrix, axis=0)
            
            ax2.plot(runs[0]['time'], avg_data, 
                    color=props['color'], linewidth=3, 
                    label=compression)
            ax2.fill_between(runs[0]['time'], 
                           avg_data - std_data, 
                           avg_data + std_data,
                           color=props['color'], alpha=0.2)
    ax2.set_title('Layer 2: + Confidence Intervals')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Layer 3: Add individual runs for selected algorithms
    ax3 = axes[1, 0]
    selected_algos = ['None', 'Snappy', 'Zstd(1)']
    for compression in selected_algos:
        if compression in compressions:
            props = compressions[compression]
            runs = data.get((compression, entry_size), [])
            if runs:
                # Individual runs
                for run in runs:
                    ax3.plot(run['time'], run[metric], 
                           color=props['color'], alpha=0.3, linewidth=1)
                # Average
                avg_data = np.mean([run[metric] for run in runs], axis=0)
                ax3.plot(runs[0]['time'], avg_data, 
                        color=props['color'], linewidth=3, 
                        label=compression)
    ax3.set_title('Layer 3: + Individual Runs (Selected)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Layer 4: Full detail
    ax4 = axes[1, 1]
    for compression, props in compressions.items():
        runs = data.get((compression, entry_size), [])
        if runs:
            # Individual runs
            for i, run in enumerate(runs):
                alpha = 0.3
                linewidth = 1
                ax4.plot(run['time'], run[metric], 
                        color=props['color'], alpha=alpha, linewidth=linewidth)
            # Average
            avg_data = np.mean([run[metric] for run in runs], axis=0)
            ax4.plot(runs[0]['time'], avg_data, 
                    color=props['color'], linewidth=3, 
                    label=compression)
    ax4.set_title('Layer 4: Full Detail (All Runs)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.suptitle(f'Layered Visualization: {metric.replace("_", " ").title()} - Entry {entry_size}B', 
                 fontsize=16, y=0.98)
    plt.savefig(f'/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/prototype_layered_{metric}.png', 
                dpi=150, bbox_inches='tight')
    plt.close()

# Approach 6: Statistical Summary with Detail Panel
def plot_statistical_summary_with_details(data, compressions):
    """Statistical summary with detailed breakdowns"""
    
    fig = plt.figure(figsize=(20, 12))
    
    # Create complex layout
    gs = fig.add_gridspec(3, 4, height_ratios=[2, 1, 1], width_ratios=[3, 1, 1, 1])
    
    # Main plot: Time series with confidence intervals
    ax_main = fig.add_subplot(gs[0, :3])
    
    entry_size = '4096'
    metric = 'throughput'
    
    for compression, props in compressions.items():
        runs = data.get((compression, entry_size), [])
        if runs:
            data_matrix = np.array([run[metric] for run in runs])
            avg_data = np.mean(data_matrix, axis=0)
            std_data = np.std(data_matrix, axis=0)
            
            ax_main.plot(runs[0]['time'], avg_data, 
                        color=props['color'], linewidth=3, 
                        label=f'{compression} (avg)')
            ax_main.fill_between(runs[0]['time'], 
                               avg_data - 1.96*std_data/np.sqrt(len(runs)), 
                               avg_data + 1.96*std_data/np.sqrt(len(runs)),
                               color=props['color'], alpha=0.2,
                               label=f'{compression} (95% CI)')
    
    ax_main.set_title('Main Plot: Group Averages ± 95% Confidence Intervals')
    ax_main.set_xlabel('Time (seconds)')
    ax_main.set_ylabel('Throughput (ops/sec)')
    ax_main.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax_main.grid(True, alpha=0.3)
    
    # Detail panels for each metric
    detail_metrics = ['peak_throughput', 'avg_cpu', 'total_cache_misses']
    detail_titles = ['Peak Throughput', 'Average CPU %', 'Total Cache Misses']
    
    for idx, (detail_metric, title) in enumerate(zip(detail_metrics, detail_titles)):
        ax_detail = fig.add_subplot(gs[idx+1, 3])
        
        # Create summary table
        summary_data = []
        for compression in compressions.keys():
            runs = data.get((compression, entry_size), [])
            if runs:
                values = [run[detail_metric] for run in runs]
                summary_data.append([
                    compression,
                    f'{np.mean(values):.1f}',
                    f'{np.std(values):.1f}',
                    f'{np.min(values):.1f}',
                    f'{np.max(values):.1f}'
                ])
        
        # Create table
        table = ax_detail.table(cellText=summary_data,
                               colLabels=['Algo', 'Mean', 'Std', 'Min', 'Max'],
                               cellLoc='center',
                               loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1, 1.5)
        
        ax_detail.set_title(title, fontsize=10)
        ax_detail.axis('off')
    
    # Performance ranking
    ax_ranking = fig.add_subplot(gs[1:, :2])
    
    # Calculate overall performance score (higher throughput, lower CPU, lower cache misses)
    scores = {}
    for compression in compressions.keys():
        runs = data.get((compression, entry_size), [])
        if runs:
            throughput_score = np.mean([run['peak_throughput'] for run in runs])
            cpu_penalty = np.mean([run['avg_cpu'] for run in runs])
            cache_penalty = np.mean([run['total_cache_misses'] for run in runs]) / 1000
            
            # Normalize and combine (higher is better)
            overall_score = throughput_score - cpu_penalty - cache_penalty
            scores[compression] = overall_score
    
    # Sort by score
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    
    # Bar chart
    compressions_sorted = [item[0] for item in sorted_scores]
    scores_sorted = [item[1] for item in sorted_scores]
    colors_sorted = [compressions[comp]['color'] for comp in compressions_sorted]
    
    bars = ax_ranking.bar(compressions_sorted, scores_sorted, color=colors_sorted, alpha=0.7)
    ax_ranking.set_title('Performance Ranking (Throughput - CPU - Cache Misses/1000)')
    ax_ranking.set_ylabel('Overall Score')
    ax_ranking.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, score in zip(bars, scores_sorted):
        height = bar.get_height()
        ax_ranking.text(bar.get_x() + bar.get_width()/2., height,
                       f'{score:.0f}',
                       ha='center', va='bottom')
    
    plt.tight_layout()
    plt.suptitle(f'Statistical Summary with Details - Entry {entry_size}B', 
                 fontsize=16, y=0.98)
    plt.savefig('/home/skarim/Code/smash/haura/betree/haura-benchmarks/haura-plots/prototype_statistical_summary.png', 
                dpi=150, bbox_inches='tight')
    plt.close()

def main():
    """Generate all prototype visualizations"""
    print("Generating sample data...")
    data, compressions = generate_sample_data()
    
    print("Creating visualization prototypes...")
    
    # Generate all approaches
    print("1. Small Multiples with Run Overlays...")
    plot_small_multiples_with_overlays(data, compressions, 'throughput')
    plot_small_multiples_with_overlays(data, compressions, 'cpu_usage')
    
    print("2. Individual Lines with Group Clustering...")
    plot_grouped_lines(data, compressions, 'throughput')
    plot_grouped_lines(data, compressions, 'cpu_usage')
    
    print("3. Box Plots with Individual Points...")
    plot_boxplots_with_points(data, compressions)
    
    print("4. Heatmap with Details...")
    plot_heatmap_with_details(data, compressions)
    
    print("5. Layered Visualization...")
    plot_layered_visualization(data, compressions, 'throughput')
    plot_layered_visualization(data, compressions, 'memory_usage')
    
    print("6. Statistical Summary with Details...")
    plot_statistical_summary_with_details(data, compressions)
    
    print("\nAll prototype visualizations generated!")
    print("Check the following files:")
    print("- prototype_small_multiples_*.png")
    print("- prototype_grouped_lines_*.png") 
    print("- prototype_boxplots.png")
    print("- prototype_heatmap.png")
    print("- prototype_layered_*.png")
    print("- prototype_statistical_summary.png")

if __name__ == "__main__":
    main()