import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
import math
import sys
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Plot YCSB execution time results')
    parser.add_argument('folder_path', help='Path to the folder containing YCSB results')
    parser.add_argument('ycsb_char', help='YCSB workload character (e.g., a, b, c, d, g)')
    return parser.parse_args()

# Parse command line arguments
args = parse_arguments()
main_dir = args.folder_path
target_char = args.ycsb_char

def extract_compression_value(config_path):
    if not os.path.exists(config_path):
        return "Unknown"
    with open(config_path, "r", encoding="utf-8-sig") as f:
        config_text = f.read()
    
    # Pattern for compression with level (Zstd, Lz4)
    pattern_with_level = re.compile(
        r'compression\s*:\s*(\w+)\s*\(\s*\1\s*{\s*level\s*:\s*(\d+)\s*,?',
        re.DOTALL
    )
    match = pattern_with_level.search(config_text)
    if match:
        comp_type, level = match.groups()
        return f"{comp_type}({level})"
    
    # Pattern for Snappy (no level parameter)
    pattern_snappy = re.compile(
        r'compression\s*:\s*Snappy\s*\(\s*Snappy\s*,?\s*\)',
        re.DOTALL
    )
    if pattern_snappy.search(config_text):
        return "Snappy"
    
    # Pattern for Rle (with parameters)
    pattern_rle = re.compile(
        r'compression\s*:\s*Rle\s*\(\s*Rle\s*\{[^}]*\}\s*,?\s*\)',
        re.DOTALL
    )
    if pattern_rle.search(config_text):
        return "Rle"
    
    # Pattern for Delta (with parameters)
    pattern_delta = re.compile(
        r'compression\s*:\s*Delta\s*\(\s*Delta\s*\{[^}]*\}\s*,?\s*\)',
        re.DOTALL
    )
    if pattern_delta.search(config_text):
        return "Delta"
    
    # Pattern for None/null
    match_flat = re.search(r'compression\s*:\s*(None|null|nullptr)', config_text, re.IGNORECASE)
    if match_flat:
        return match_flat.group(1)
    
    return "Unknown"

def debug_compression_parsing(main_dir, target_char):
    """Debug function to show what compression values are being parsed"""
    print("=== DEBUG: Compression parsing ===")
    for folder in os.listdir(main_dir):
        if folder.startswith(f"ycsb_{target_char}"):
            config_path = os.path.join(main_dir, folder, "config")
            if os.path.exists(config_path):
                label = extract_compression_value(config_path)
                print(f"Folder: {folder} -> Compression: {label}")
    print("=== END DEBUG ===\n")

def extract_entry_size(folder_name):
    """Extract entry size from folder name like 'ycsb_g_entry512_none_1753381182' or 'ycsb_g_entry4k_none_1753381182'"""
    match = re.search(r'entry(\d+k?)', folder_name)
    if match:
        size_str = match.group(1)
        if size_str.endswith('k'):
            # Convert k suffix to actual bytes
            return str(int(size_str[:-1]) * 1024)
        return size_str
    return None

# Debug: Show what compression values are being parsed
# debug_compression_parsing(main_dir, target_char)

# Step 1: Collect all folders and group by entry size
compression_labels = set()
folders_by_entry_size = {}
for folder in os.listdir(main_dir):
    if folder.startswith(f"ycsb_{target_char}"):
        entry_size = extract_entry_size(folder)
        if entry_size:
            folders_by_entry_size.setdefault(entry_size, []).append(folder)
            config_path = os.path.join(main_dir, folder, "config")
            label = extract_compression_value(config_path)
            compression_labels.add(label)

# Dynamically determine entry sizes and their positions
available_entry_sizes = sorted([int(size) for size in folders_by_entry_size.keys()])
entry_size_order = [str(size) for size in available_entry_sizes]

# print(f"DEBUG: Detected entry sizes: {folders_by_entry_size.keys()}")
# print(f"DEBUG: Available entry sizes (sorted): {available_entry_sizes}")
# print(f"DEBUG: Entry size order: {entry_size_order}")

# Create positions dynamically based on available entry sizes
entry_size_positions = {}
if len(available_entry_sizes) == 1:
    entry_size_positions[entry_size_order[0]] = (0, 0)
elif len(available_entry_sizes) == 2:
    entry_size_positions[entry_size_order[0]] = (0, 0)
    entry_size_positions[entry_size_order[1]] = (0, 1)
elif len(available_entry_sizes) == 3:
    entry_size_positions[entry_size_order[0]] = (0, 0)
    entry_size_positions[entry_size_order[1]] = (0, 1)
    entry_size_positions[entry_size_order[2]] = (1, 0)
else:  # 4 or more
    for i, size in enumerate(entry_size_order[:4]):  # Only show first 4
        row = i // 2
        col = i % 2
        entry_size_positions[size] = (row, col)

# Assign consistent color per label
color_map = plt.get_cmap("tab10")
label_list = sorted(list(compression_labels))
label_colors = {label: color_map(i % 10) for i, label in enumerate(label_list)}

# Step 2: Setup subplot layout based on number of entry sizes
num_entry_sizes = len(available_entry_sizes)
if num_entry_sizes == 1:
    fig, axs = plt.subplots(1, 1, figsize=(8, 6))
    axs = np.array([[axs]])  # Make it 2D numpy array for consistent indexing
elif num_entry_sizes == 2:
    fig, axs = plt.subplots(1, 2, figsize=(16, 6))
    axs = np.array([axs])  # Make it 2D numpy array for consistent indexing
elif num_entry_sizes == 3:
    fig, axs = plt.subplots(2, 2, figsize=(16, 12))
    axs[1, 1].set_visible(False)  # Hide the unused subplot
else:  # 4 or more
    fig, axs = plt.subplots(2, 2, figsize=(16, 12))

all_labels_used = set()

title_map = {
    "a": "YCSB-A (Read Heavy)",
    "b": "YCSB-B (Read/Write Mix)",
    "c": "YCSB-C (Read Only)",
    "d": "YCSB-D (Read Latest)",
    "e": "YCSB-E (Scan)",
    "f": "YCSB-F (Read-Modify-Write)",
    "g": "YCSB-G (Update Heavy)",
    "h": "YCSB-H (Mixed)",
    "i": "YCSB-I (Insert Heavy)",
    # Add more mappings as needed
}

plot_title = title_map.get(target_char, f"YCSB-{target_char.upper()}")
fig.suptitle(f"{plot_title} - Operations/sec by Thread Count (Different Entry Sizes)", fontsize=18, y=0.95)

# Step 1: Define label order and fixed colors
preferred_order = ["None", "Snappy", "Rle", "Delta", "Zstd(1)", "Zstd(5)", "Zstd(10)", "Lz4(1)", "Lz4(5)", "Lz4(10)"]
label_list = preferred_order  # For legend consistency

label_colors = {
    "None": "#333333",       # Dark gray
    "Snappy": "#2ca02c",     # Green
    "Rle": "#d62728",        # Red
    "Delta": "#9467bd",      # Purple
    "Zstd(1)": "#1f77b4",     # Deep blue
    "Zstd(5)": "#5fa2dc",     # Medium blue
    "Zstd(10)": "#a6c8ed",    # Light blue
    "Lz4(1)": "#ff7f0e",      # Bold orange
    "Lz4(5)": "#ffae64",      # Light orange
    "Lz4(10)": "#ffd5b2"      # Pale peach
}

# Step 3: Scan for global max ops/sec across all entry sizes
global_max_ops = 0
for entry_size, folders in folders_by_entry_size.items():
    for folder in folders:
        csv_file = os.path.join(main_dir, folder, f"ycsb_{target_char}.csv")
        if os.path.exists(csv_file):
            try:
                df = pd.read_csv(csv_file)
                if not df.empty and "ops" in df.columns:
                    max_val = df["ops"].max()
                    global_max_ops = max(global_max_ops, max_val)
            except (pd.errors.EmptyDataError, pd.errors.ParserError):
                continue  # Skip malformed or empty CSV files

# Step 4: Create subplots for each entry size
for entry_size in entry_size_order:
    if entry_size not in folders_by_entry_size:
        # If no data for this entry size, hide the subplot
        row, col = entry_size_positions[entry_size]
        axs[row, col].set_visible(False)
        continue
    
    folders = folders_by_entry_size[entry_size]
    ops_by_label = {}
    all_thread_counts = set()
    
    # Step 4.1: Collect ops/sec per label and thread count for this entry size
    for folder in folders:
        folder_path = os.path.join(main_dir, folder)
        csv_file = os.path.join(folder_path, f"ycsb_{target_char}.csv")
        config_path = os.path.join(folder_path, "config")
        label = extract_compression_value(config_path)

        if not os.path.exists(csv_file) or label not in preferred_order:
            continue

        try:
            df = pd.read_csv(csv_file)
            if df.empty or "ops" not in df.columns or "threads" not in df.columns:
                continue
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            continue  # Skip malformed or empty CSV files

        thread = int(df["threads"].values[0])
        ops = float(df["ops"].values[0])
        all_thread_counts.add(thread)

        ops_by_label.setdefault(label, {})[thread] = ops
        all_labels_used.add(label)

    if not ops_by_label:
        # If no valid data for this entry size, hide the subplot
        row, col = entry_size_positions[entry_size]
        axs[row, col].set_visible(False)
        continue

    # Step 4.2: Prepare sorted thread counts
    sorted_threads = sorted(all_thread_counts)
    base_x = np.arange(len(sorted_threads))
    total_group_width = 0.8
    bar_width = total_group_width / len(preferred_order)

    # Step 4.3: Plot bars per label
    row, col = entry_size_positions[entry_size]
    ax = axs[row, col]
    
    for i, label in enumerate(preferred_order):
        thread_ops = ops_by_label.get(label, {})
        values = [thread_ops.get(t, 0) for t in sorted_threads]
        offsets = base_x - (total_group_width / 2) + i * bar_width
        ax.bar(offsets, values, width=bar_width, color=label_colors[label], label=label)

    # Step 4.4: Finalize subplot
    midpoints = base_x  # This centers the labels under each group of bars
    ax.set_xticks(midpoints)
    ax.set_xticklabels(sorted_threads)
    ax.set_xlabel("Threads")
    ax.set_ylabel("Ops/sec")
    ax.set_ylim(0, global_max_ops * 1.1)
    ax.grid(axis="y", linestyle="dotted", alpha=0.6)
    ax.set_title(f"Entry Size: {entry_size} bytes", fontsize=14)

# Check if we have any data at all
if not all_labels_used:
    print(f"No data found for YCSB-{target_char} in {main_dir}")
    sys.exit(1)


# Step 5: Add global legend
handles = [plt.Rectangle((0, 0), 1, 1, color=label_colors[label]) for label in label_list if label in all_labels_used]
fig.legend(handles, [label for label in label_list if label in all_labels_used],
           fontsize="medium", loc='center right', bbox_to_anchor=(0.98, 0.5))

plt.tight_layout(rect=[0, 0, 0.85, 0.92])  # Leave space for the title and legend
output_filename = os.path.join(main_dir, f"ycsb_{target_char}_entry_sizes_ops_per_thread.png")
plt.savefig(output_filename, dpi=300, bbox_inches='tight')
print(f"Plot saved as: {output_filename}")
plt.show()