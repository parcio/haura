#!/usr/bin/env python3
"""
Generate a summary report of the loaded benchmark data
"""

from simple_heatmap_analyzer import SimpleHauraBenchmarkAnalyzer

def main():
    # Create analyzer and load data
    analyzer = SimpleHauraBenchmarkAnalyzer('/home/skarim/Code/smash/haura/betree/haura-benchmarks/results/2025-07-24_default')
    analyzer.load_all_data()
    
    print("=" * 80)
    print("HAURA BENCHMARK DATA SUMMARY")
    print("=" * 80)
    
    print(f"\nTOTAL CONFIGURATIONS: {len(analyzer.data)}")
    print(f"   • {len(analyzer.compression_algorithms)} compression algorithms")
    print(f"   • {len(analyzer.entry_sizes)} entry sizes") 
    print(f"   • {len(analyzer.thread_counts)} thread counts")
    print(f"   • Expected total: {len(analyzer.compression_algorithms)} × {len(analyzer.entry_sizes)} × {len(analyzer.thread_counts)} = {len(analyzer.compression_algorithms) * len(analyzer.entry_sizes) * len(analyzer.thread_counts)}")
    
    print(f"\nCOMPRESSION ALGORITHMS:")
    for comp in analyzer.compression_algorithms:
        count = sum(1 for key in analyzer.data.keys() if key[0] == comp)
        print(f"   • {comp}: {count} runs")
    
    print(f"\n📏 ENTRY SIZES:")
    for size in analyzer.entry_sizes:
        count = sum(1 for key in analyzer.data.keys() if key[1] == size)
        print(f"   • {size}B: {count} runs")
    
    print(f"\n🧵 THREAD COUNTS:")
    for threads in analyzer.thread_counts:
        count = sum(1 for key in analyzer.data.keys() if key[2] == threads)
        print(f"   • {threads} threads: {count} runs")
    
    # Sample some data points
    print(f"\nSAMPLE DATA POINTS:")
    sample_keys = list(analyzer.data.keys())[:5]
    for key in sample_keys:
        compression, entry_size, threads = key
        data = analyzer.data[key]
        print(f"   • {compression}, {entry_size}B, {threads} threads:")
        print(f"     - Throughput: {data.get('throughput', 'N/A'):.0f} ops/sec" if data.get('throughput') else "     - Throughput: N/A")
        print(f"     - Cache hits: {data.get('cache_hits', 'N/A')}")
        print(f"     - Cache misses: {data.get('cache_misses', 'N/A')}")
        print(f"     - Storage written: {data.get('storage_written', 'N/A')} bytes")
    
    # Find some interesting patterns
    print(f"\nINTERESTING PATTERNS:")
    
    # Find highest throughput
    max_throughput = 0
    max_throughput_config = None
    for key, data in analyzer.data.items():
        if data.get('throughput', 0) > max_throughput:
            max_throughput = data['throughput']
            max_throughput_config = key
    
    if max_throughput_config:
        compression, entry_size, threads = max_throughput_config
        print(f"   • Highest throughput: {max_throughput:.0f} ops/sec")
        print(f"     Configuration: {compression}, {entry_size}B, {threads} threads")
    
    # Find lowest cache misses
    min_cache_misses = float('inf')
    min_cache_misses_config = None
    for key, data in analyzer.data.items():
        cache_misses = data.get('cache_misses', float('inf'))
        if cache_misses < min_cache_misses:
            min_cache_misses = cache_misses
            min_cache_misses_config = key
    
    if min_cache_misses_config and min_cache_misses != float('inf'):
        compression, entry_size, threads = min_cache_misses_config
        print(f"   • Lowest cache misses: {min_cache_misses}")
        print(f"     Configuration: {compression}, {entry_size}B, {threads} threads")
    
    # Find lowest storage written
    min_storage = float('inf')
    min_storage_config = None
    for key, data in analyzer.data.items():
        storage = data.get('storage_written', float('inf'))
        if storage < min_storage:
            min_storage = storage
            min_storage_config = key
    
    if min_storage_config and min_storage != float('inf'):
        compression, entry_size, threads = min_storage_config
        print(f"   • Lowest storage written: {min_storage} bytes")
        print(f"     Configuration: {compression}, {entry_size}B, {threads} threads")
    
    print(f"\nDATA LOADED SUCCESSFULLY!")
    print(f"   Ready for heatmap visualization in heatmap_analysis.html")
    print("=" * 80)

if __name__ == "__main__":
    main()