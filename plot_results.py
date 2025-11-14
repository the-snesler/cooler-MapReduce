#!/usr/bin/env python3
"""
Generate plots from benchmark results.
"""

import json
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import numpy as np
from collections import defaultdict

# Configuration
PLOTS_DIR = Path("benchmark_results/plots")


def load_results(json_file):
    """Load benchmark results from JSON file."""
    with open(json_file, 'r') as f:
        return json.load(f)


def aggregate_runs(results):
    """
    Aggregate multiple runs of the same benchmark.
    Returns dict: benchmark_name -> {avg_runtime, std_runtime, config, ...}
    """
    by_benchmark = defaultdict(list)

    for r in results:
        if r['success']:  # Only include successful runs
            by_benchmark[r['benchmark_name']].append(r)

    aggregated = {}
    for name, runs in by_benchmark.items():
        if not runs:
            continue

        runtimes = [r['total_runtime_seconds'] for r in runs]
        throughputs = [r['throughput_mbps'] for r in runs]

        # Use first run for configuration data
        first = runs[0]

        aggregated[name] = {
            'benchmark_name': name,
            'description': first['description'],
            'num_map_tasks': first['num_map_tasks'],
            'num_reduce_tasks': first['num_reduce_tasks'],
            'input_size_mb': first['input_size_mb'],
            'avg_runtime': np.mean(runtimes),
            'std_runtime': np.std(runtimes),
            'min_runtime': np.min(runtimes),
            'max_runtime': np.max(runtimes),
            'avg_throughput': np.mean(throughputs),
            'num_runs': len(runs)
        }

    return aggregated


def plot_input_size_scaling(aggregated, output_file):
    """Plot runtime vs input size."""
    # Filter benchmarks from input_size experiment
    data = [(v['input_size_mb'], v['avg_runtime'], v['std_runtime'])
            for k, v in aggregated.items()
            if k.startswith('input_size_')]

    if not data:
        print("⚠️  No input size scaling data found")
        return

    data.sort()  # Sort by input size
    sizes, runtimes, stds = zip(*data)

    plt.figure(figsize=(10, 6))
    plt.errorbar(sizes, runtimes, yerr=stds, marker='o', capsize=5,
                 linewidth=2, markersize=8)
    plt.xlabel('Input Size (MB)', fontsize=12)
    plt.ylabel('Runtime (seconds)', fontsize=12)
    plt.title('MapReduce Performance: Input Size Scaling\n(4 map tasks, 2 reduce tasks)',
              fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def plot_map_task_scaling(aggregated, output_file):
    """Plot runtime vs number of map tasks."""
    data = [(v['num_map_tasks'], v['avg_runtime'], v['std_runtime'])
            for k, v in aggregated.items()
            if k.startswith('map_scaling_')]

    if not data:
        print("⚠️  No map scaling data found")
        return

    data.sort()
    map_tasks, runtimes, stds = zip(*data)

    plt.figure(figsize=(10, 6))
    plt.errorbar(map_tasks, runtimes, yerr=stds, marker='s', capsize=5,
                 linewidth=2, markersize=8, color='orangered')
    plt.xlabel('Number of Map Tasks', fontsize=12)
    plt.ylabel('Runtime (seconds)', fontsize=12)
    plt.title('MapReduce Performance: Map Task Parallelism\n(~10MB input, 2 reduce tasks)',
              fontsize=14, fontweight='bold')
    plt.xticks(map_tasks)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def plot_reduce_task_scaling(aggregated, output_file):
    """Plot runtime vs number of reduce tasks."""
    data = [(v['num_reduce_tasks'], v['avg_runtime'], v['std_runtime'])
            for k, v in aggregated.items()
            if k.startswith('reduce_scaling_')]

    if not data:
        print("⚠️  No reduce scaling data found")
        return

    data.sort()
    reduce_tasks, runtimes, stds = zip(*data)

    plt.figure(figsize=(10, 6))
    plt.errorbar(reduce_tasks, runtimes, yerr=stds, marker='^', capsize=5,
                 linewidth=2, markersize=8, color='green')
    plt.xlabel('Number of Reduce Tasks', fontsize=12)
    plt.ylabel('Runtime (seconds)', fontsize=12)
    plt.title('MapReduce Performance: Reduce Task Parallelism\n(~10MB input, 4 map tasks)',
              fontsize=14, fontweight='bold')
    plt.xticks(reduce_tasks)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def plot_speedup(aggregated, output_file):
    """Plot speedup for map task scaling."""
    data = [(v['num_map_tasks'], v['avg_runtime'])
            for k, v in aggregated.items()
            if k.startswith('map_scaling_')]

    if not data or len(data) < 2:
        print("⚠️  Insufficient data for speedup plot")
        return

    data.sort()
    map_tasks, runtimes = zip(*data)

    # Calculate speedup relative to 1 task
    baseline = runtimes[0]
    speedups = [baseline / rt for rt in runtimes]
    ideal_speedup = list(map_tasks)  # Linear speedup

    plt.figure(figsize=(10, 6))
    plt.plot(map_tasks, speedups, marker='o', linewidth=2, markersize=8,
             label='Actual Speedup', color='blue')
    plt.plot(map_tasks, ideal_speedup, linestyle='--', linewidth=2,
             label='Ideal (Linear) Speedup', color='gray', alpha=0.7)
    plt.xlabel('Number of Map Tasks', fontsize=12)
    plt.ylabel('Speedup', fontsize=12)
    plt.title('MapReduce Speedup vs Ideal Linear Speedup',
              fontsize=14, fontweight='bold')
    plt.xticks(map_tasks)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def plot_combined_heatmap(aggregated, output_file):
    """Plot heatmap of runtime for different (map, reduce) combinations."""
    data = [(v['num_map_tasks'], v['num_reduce_tasks'], v['avg_runtime'])
            for k, v in aggregated.items()
            if k.startswith('combined_')]

    if not data:
        print("⚠️  No combined scaling data found")
        return

    # Build matrix
    map_values = sorted(set(d[0] for d in data))
    reduce_values = sorted(set(d[1] for d in data))

    matrix = np.zeros((len(reduce_values), len(map_values)))
    for m, r, runtime in data:
        i = reduce_values.index(r)
        j = map_values.index(m)
        matrix[i, j] = runtime

    plt.figure(figsize=(10, 8))
    im = plt.imshow(matrix, cmap='YlOrRd', aspect='auto')

    plt.xticks(range(len(map_values)), map_values)
    plt.yticks(range(len(reduce_values)), reduce_values)
    plt.xlabel('Number of Map Tasks', fontsize=12)
    plt.ylabel('Number of Reduce Tasks', fontsize=12)
    plt.title('MapReduce Runtime Heatmap (seconds)\n(~50MB input)',
              fontsize=14, fontweight='bold')

    # Add colorbar
    cbar = plt.colorbar(im)
    cbar.set_label('Runtime (seconds)', fontsize=11)

    # Annotate cells with values
    for i in range(len(reduce_values)):
        for j in range(len(map_values)):
            if matrix[i, j] > 0:
                text = plt.text(j, i, f'{matrix[i, j]:.1f}',
                               ha="center", va="center", color="black", fontsize=10)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def generate_summary_table(aggregated, output_file):
    """Generate a markdown table summarizing all results."""
    lines = [
        "# Benchmark Results Summary\n",
        "| Benchmark | Maps | Reduces | Input (MB) | Avg Runtime (s) | Std Dev | Throughput (MB/s) |",
        "|-----------|------|---------|------------|-----------------|---------|-------------------|"
    ]

    for name in sorted(aggregated.keys()):
        v = aggregated[name]
        lines.append(
            f"| {v['benchmark_name']:<21} | {v['num_map_tasks']:>4} | "
            f"{v['num_reduce_tasks']:>7} | {v['input_size_mb']:>10.2f} | "
            f"{v['avg_runtime']:>15.2f} | {v['std_runtime']:>7.3f} | "
            f"{v['avg_throughput']:>17.3f} |"
        )

    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))

    print(f"✓ Saved: {output_file}")


def main():
    """Generate all plots from benchmark results."""
    if len(sys.argv) < 2:
        print("Usage: python plot_results.py <results.json>")
        print("\nExample:")
        print("  python plot_results.py benchmark_results/benchmark_results_20250113_120000.json")
        sys.exit(1)

    json_file = sys.argv[1]

    if not Path(json_file).exists():
        print(f"❌ File not found: {json_file}")
        sys.exit(1)

    print(f"Loading results from: {json_file}")
    results = load_results(json_file)
    print(f"✓ Loaded {len(results)} benchmark results")

    # Aggregate runs
    aggregated = aggregate_runs(results)
    print(f"✓ Aggregated into {len(aggregated)} unique benchmarks")

    # Create output directory
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # Generate plots
    print("\nGenerating plots...")
    plot_input_size_scaling(aggregated, PLOTS_DIR / "1_input_size_scaling.png")
    plot_map_task_scaling(aggregated, PLOTS_DIR / "2_map_task_scaling.png")
    plot_reduce_task_scaling(aggregated, PLOTS_DIR / "3_reduce_task_scaling.png")
    plot_speedup(aggregated, PLOTS_DIR / "4_speedup_analysis.png")
    plot_combined_heatmap(aggregated, PLOTS_DIR / "5_combined_heatmap.png")

    # Generate summary table
    generate_summary_table(aggregated, PLOTS_DIR / "results_table.md")

    print(f"\n{'='*70}")
    print(f"All plots saved to: {PLOTS_DIR}/")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
