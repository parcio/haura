#!/usr/bin/env python3
"""
Generate metrics plots for all benchmark runs using existing metrics_plots.py functions
"""

import os
import sys
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import argparse

# Add the haura_plots directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

try:
    # Try to import matplotlib and numpy first
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import numpy as np
    
    # Add haura_plots to path and import
    haura_plots_path = str(Path(__file__).parent / "haura_plots")
    if haura_plots_path not in sys.path:
        sys.path.insert(0, haura_plots_path)
    
    # Import util first
    import util
    
    # Now import the plotting functions with util available
    import metrics_plots
    plot_throughput = metrics_plots.plot_throughput
    plot_tier_usage = metrics_plots.plot_tier_usage
    plot_system = metrics_plots.plot_system
    
except Exception as e:
    print(f"❌ Error importing required modules: {e}")
    print("Make sure you have matplotlib and numpy installed:")
    print("  pip install matplotlib numpy")
    print("Also ensure you're running this script from the haura-plots directory")
    sys.exit(1)

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
                "INFO": "ℹ️ ",
                "SUCCESS": "✅",
                "WARNING": "⚠️ ",
                "ERROR": "❌"
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
        self.log("🚀 Starting metrics plot generation...")
        
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
        print("📊 METRICS PLOT GENERATION SUMMARY")
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
            print("✅ Plot generation completed!")
            print("📁 Check individual run folders for generated SVG files:")
            print("   - plot_write.svg (write throughput)")
            print("   - plot_read.svg (read throughput)")  
            print("   - tier_usage.svg (storage tier usage)")
            print("   - proc.svg (system metrics)")
        else:
            print("⚠️  No plots were generated.")

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
            print("❌ No default results directories found!")
            print("Please specify results directories with --results-dir")
            return 1
    
    # Create generator and run
    generator = MetricsPlotGenerator(args.results_dirs, verbose=not args.quiet)
    generator.generate_all_plots(overwrite=args.overwrite)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())