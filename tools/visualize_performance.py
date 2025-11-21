#!/usr/bin/env python3
"""
Generate performance visualization plots.
"""

import matplotlib.pyplot as plt
import json
import sys


def plot_combiner_comparison(metrics_with, metrics_without):
    """Create bar chart comparing combiner vs no combiner."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Execution time comparison
    labels = ['Without Combiner', 'With Combiner']
    times = [
        metrics_without['total_time_seconds'],
        metrics_with['total_time_seconds']
    ]

    ax1.bar(labels, times, color=['#FF6B6B', '#4ECDC4'])
    ax1.set_ylabel('Execution Time (seconds)')
    ax1.set_title('Job Execution Time Comparison')
    ax1.grid(axis='y', alpha=0.3)

    # Intermediate data size comparison
    sizes = [
        metrics_without['intermediate_size_bytes'] / (1024**2),  # Convert to MB
        metrics_with['intermediate_size_bytes'] / (1024**2)
    ]

    ax2.bar(labels, sizes, color=['#FF6B6B', '#4ECDC4'])
    ax2.set_ylabel('Intermediate Data Size (MB)')
    ax2.set_title('Intermediate Data Size Comparison')
    ax2.grid(axis='y', alpha=0.3)

    # Add reduction percentage
    if sizes[0] > 0:
        reduction = ((sizes[0] - sizes[1]) / sizes[0]) * 100
        ax2.text(0.5, max(sizes) * 0.9, f'{reduction:.1f}% reduction',
                 ha='center', fontsize=12, fontweight='bold')

    plt.tight_layout()
    plt.savefig('performance_comparison.png', dpi=300)
    print("Saved plot to performance_comparison.png")


def plot_phase_breakdown(metrics):
    """Create pie chart showing phase time breakdown."""
    map_time = metrics['map_phase_time_seconds']
    reduce_time = metrics['reduce_phase_time_seconds']

    fig, ax = plt.subplots(figsize=(8, 6))

    sizes = [map_time, reduce_time]
    labels = [f'Map Phase\n({map_time:.2f}s)', f'Reduce Phase\n({reduce_time:.2f}s)']
    colors = ['#FFE66D', '#95E1D3']

    ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%',
           startangle=90, textprops={'fontsize': 12})
    ax.set_title('Job Execution Phase Breakdown', fontsize=14, fontweight='bold')

    plt.tight_layout()
    plt.savefig('phase_breakdown.png', dpi=300)
    print("Saved plot to phase_breakdown.png")


def plot_detailed_comparison(metrics_with, metrics_without):
    """Create detailed comparison showing multiple metrics."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))

    labels = ['Without\nCombiner', 'With\nCombiner']

    # 1. Total execution time
    times = [
        metrics_without['total_time_seconds'],
        metrics_with['total_time_seconds']
    ]
    ax1.bar(labels, times, color=['#FF6B6B', '#4ECDC4'])
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('Total Execution Time')
    ax1.grid(axis='y', alpha=0.3)

    # 2. Phase breakdown
    map_times = [
        metrics_without['map_phase_time_seconds'],
        metrics_with['map_phase_time_seconds']
    ]
    reduce_times = [
        metrics_without['reduce_phase_time_seconds'],
        metrics_with['reduce_phase_time_seconds']
    ]

    x = range(len(labels))
    width = 0.35
    ax2.bar([i - width/2 for i in x], map_times, width, label='Map Phase', color='#FFE66D')
    ax2.bar([i + width/2 for i in x], reduce_times, width, label='Reduce Phase', color='#95E1D3')
    ax2.set_ylabel('Time (seconds)')
    ax2.set_title('Phase Time Breakdown')
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)

    # 3. Data size comparison
    input_size = metrics_without['input_size_bytes'] / (1024**2)
    intermediate_sizes = [
        metrics_without['intermediate_size_bytes'] / (1024**2),
        metrics_with['intermediate_size_bytes'] / (1024**2)
    ]
    output_sizes = [
        metrics_without['output_size_bytes'] / (1024**2),
        metrics_with['output_size_bytes'] / (1024**2)
    ]

    x = range(len(labels))
    width = 0.25
    ax3.bar([i - width for i in x], [input_size, input_size], width, label='Input', color='#A8E6CF')
    ax3.bar(x, intermediate_sizes, width, label='Intermediate', color='#FFD3B6')
    ax3.bar([i + width for i in x], output_sizes, width, label='Output', color='#FFAAA5')
    ax3.set_ylabel('Size (MB)')
    ax3.set_title('Data Size Comparison')
    ax3.set_xticks(x)
    ax3.set_xticklabels(labels)
    ax3.legend()
    ax3.grid(axis='y', alpha=0.3)

    # 4. Combiner effectiveness
    if 'combiner_reduction_ratio' in metrics_with:
        ratio_with = metrics_with['combiner_reduction_ratio'] * 100
        ratio_without = metrics_without.get('combiner_reduction_ratio', 0) * 100

        ax4.bar(labels, [ratio_without, ratio_with], color=['#FF6B6B', '#4ECDC4'])
        ax4.set_ylabel('Reduction Ratio (%)')
        ax4.set_title('Combiner Effectiveness')
        ax4.grid(axis='y', alpha=0.3)
        ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.5)

    plt.tight_layout()
    plt.savefig('detailed_comparison.png', dpi=300)
    print("Saved plot to detailed_comparison.png")


def main():
    if len(sys.argv) < 3:
        print("Usage: python3 visualize_performance.py <metrics_with_combiner.json> <metrics_without_combiner.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        metrics_with = json.load(f)

    with open(sys.argv[2]) as f:
        metrics_without = json.load(f)

    print("Generating visualizations...")
    plot_combiner_comparison(metrics_with, metrics_without)
    plot_phase_breakdown(metrics_with)
    plot_detailed_comparison(metrics_with, metrics_without)
    print("\nDone! Generated 3 visualization files:")
    print("  - performance_comparison.png")
    print("  - phase_breakdown.png")
    print("  - detailed_comparison.png")


if __name__ == '__main__':
    main()
