# MapReduce Performance Evaluation Report

## Executive Summary

This report presents performance evaluation of the Coolest MapReduce implementation, with focus on combiner optimization effectiveness and system scalability.

**Key Findings:**

- Combiner optimization reduced intermediate data by **[X%]**
- Job completion time improved by **[Y%]** with combiner enabled
- Optimal configuration for Shakespeare dataset: **M=[optimal_M], R=[optimal_R]**
- System demonstrated **[scaling characteristics]** with varying parallelism

**Recommendations:**

- Use combiner for jobs with commutative, associative reduce functions
- Set M to 2-4x number of workers for optimal parallelism
- Monitor intermediate data size to identify optimization opportunities

## 1. Experimental Setup

### Hardware Environment

- **Platform**: [e.g., AWS EC2 t3.2xlarge, local VM, etc.]
- **CPU**: [e.g., 8 cores @ 2.5 GHz]
- **Memory**: [e.g., 16 GB RAM]
- **Storage**: [e.g., SSD, 200 GB]
- **Operating System**: [e.g., Ubuntu 22.04 LTS]

### Software Configuration

- **Docker**: Version [e.g., 24.0.7]
- **Docker Compose**: Version [e.g., 2.23.0]
- **Python**: Version 3.9+
- **Worker containers**: 4 workers, each limited to 1 CPU core

### Dataset

- **Source**: Shakespeare Complete Works
- **File**: `/mapreduce-data/inputs/shakespeare.txt`
- **Size**: [e.g., 5.3 MB]
- **Lines**: [e.g., ~147,000 lines]
- **Words**: [e.g., ~900,000 words]
- **Unique words**: [e.g., ~30,000 unique words]

### Job Configuration

**Word Count Job:**
- Map function: Tokenize lines, emit (word, 1)
- Reduce function: Sum counts per word
- Combiner function: Sum counts locally (same as reduce)

**Test Configurations:**

| Configuration | M (Map Tasks) | R (Reduce Tasks) | Combiner |
|---------------|---------------|------------------|----------|
| Baseline      | 8             | 4                | No       |
| Optimized     | 8             | 4                | Yes      |
| Scalability 1 | 2             | 4                | Yes      |
| Scalability 2 | 4             | 4                | Yes      |
| Scalability 3 | 16            | 4                | Yes      |
| Scalability 4 | 32            | 4                | Yes      |

### Methodology

1. **Combiner Effectiveness**: Compare identical jobs with/without combiner
2. **Scalability Analysis**: Vary M from 2 to 32, measure execution time
3. **Metrics Collection**: Using built-in `coordinator/metrics.py` system
4. **Repetitions**: Each configuration run 3 times, results averaged
5. **Warmup**: One warmup run before measurements to warm caches

## 2. Results

### 2.1 Combiner Effectiveness

#### Execution Time Comparison

![Performance Comparison](performance_comparison.png)

| Metric | Without Combiner | With Combiner | Improvement |
|--------|------------------|---------------|-------------|
| **Total Execution Time** | [X.XX s] | [Y.YY s] | [Z.Z%] |
| **Map Phase Duration** | [X.XX s] | [Y.YY s] | [Z.Z%] |
| **Reduce Phase Duration** | [X.XX s] | [Y.YY s] | [Z.Z%] |

**Analysis:**

[Interpretation of timing results. Example: "The combiner reduced total execution time by 28%, with the most significant improvement in the reduce phase (35% reduction). This indicates that combiner effectively reduced data transfer and reduce task processing time."]

#### Data Size Comparison

![Data Size Comparison](data_size_comparison.png)

| Metric | Without Combiner | With Combiner | Reduction |
|--------|------------------|---------------|-----------|
| **Input Size** | [X.XX MB] | [X.XX MB] | N/A |
| **Intermediate Size** | [X.XX MB] | [Y.YY MB] | [Z.Z%] |
| **Output Size** | [X.XX MB] | [X.XX MB] | N/A |
| **Combiner Reduction Ratio** | N/A | [0.XX] | [XX%] |

**Analysis:**

[Interpretation of data size results. Example: "The combiner reduced intermediate data by 64%, from 3.2 MB to 1.15 MB. This demonstrates the effectiveness of local aggregation for word count, where many repeated words are combined at the map side before being sent to reducers."]

#### Phase Breakdown

![Phase Breakdown](phase_breakdown.png)

**Without Combiner:**
- Map phase: [XX%] of total time
- Reduce phase: [YY%] of total time
- Overhead: [ZZ%] of total time

**With Combiner:**
- Map phase: [XX%] of total time (increased due to combiner processing)
- Reduce phase: [YY%] of total time (decreased due to less data)
- Overhead: [ZZ%] of total time

**Analysis:**

[Interpretation of phase breakdown. Example: "While the combiner adds overhead to the map phase (increased from 45% to 52% of total time), it more than compensates by reducing the reduce phase from 40% to 30% of total time. The net result is 28% overall speedup."]

### 2.2 Scalability Analysis

#### Execution Time vs. Number of Map Tasks

![Scalability Plot](scalability_plot.png)

| M (Map Tasks) | Execution Time (s) | Speedup vs. M=2 |
|---------------|-------------------|-----------------|
| 2             | [X.XX]            | 1.0x            |
| 4             | [X.XX]            | [X.XX]x         |
| 8             | [X.XX]            | [X.XX]x         |
| 16            | [X.XX]            | [X.XX]x         |
| 32            | [X.XX]            | [X.XX]x         |

**Analysis:**

[Interpretation of scalability results. Example: "Execution time decreased from 18.5s (M=2) to 8.2s (M=8), achieving 2.26x speedup. However, increasing M beyond 8 showed diminishing returns, with M=16 only providing 2.4x speedup. This is expected as the system has 4 workers, limiting maximum parallelism to 4 concurrent tasks."]

#### Optimal Configuration

Based on experimental results:

- **Optimal M**: [e.g., 8] (2x number of workers)
- **Optimal R**: [e.g., 4] (1x number of workers)
- **Combiner**: Enabled

**Rationale:**

[Explanation of optimal configuration. Example: "M=8 provides best balance between parallelism and overhead. Higher M values (16, 32) don't improve performance significantly due to worker count limitation, but increase task scheduling overhead. R=4 matches worker count for efficient reduce phase execution."]

### 2.3 Detailed Performance Breakdown

#### Map Phase Analysis

| Metric | Value | Notes |
|--------|-------|-------|
| Average map task duration | [X.XX s] | [Notes] |
| Input read throughput | [X.XX MB/s] | [Notes] |
| Map output rate | [X.XX records/s] | [Notes] |
| Combiner overhead | [X.XX ms] | [Notes] |

**Analysis:**

[Interpretation of map phase metrics.]

#### Reduce Phase Analysis

| Metric | Value | Notes |
|--------|-------|-------|
| Average reduce task duration | [X.XX s] | [Notes] |
| Input read throughput | [X.XX MB/s] | [Notes] |
| Grouping overhead | [X.XX ms] | [Notes] |
| Sorting overhead | [X.XX ms] | [Notes] |
| Reduce output rate | [X.XX records/s] | [Notes] |

**Analysis:**

[Interpretation of reduce phase metrics.]

## 3. Discussion

### 3.1 Combiner Optimization Effectiveness

**When combiner is effective:**

- Reduce function is commutative and associative
- High data locality (many repeated keys per map task)
- Simple aggregation operations (sum, count, max, min)

**Word count case study:**

[Detailed analysis of why combiner works well for word count. Example: "Word count is an ideal use case for combiner optimization because: (1) summing is commutative and associative, (2) many words repeat within each map task, and (3) local aggregation significantly reduces distinct key-value pairs sent to reducers."]

**Limitations:**

[Discussion of scenarios where combiner may not help. Example: "Combiner is less effective for jobs with high key cardinality (few repeated keys per map task) or complex reduce functions that don't allow partial aggregation."]

### 3.2 Scalability Characteristics

**Observed scaling pattern:**

[Analysis of how the system scales. Example: "The system exhibits sub-linear scaling, typical of distributed systems with coordination overhead. Doubling M from 4 to 8 provides 1.8x speedup rather than 2x, indicating ~10% overhead from task scheduling and data shuffling."]

**Bottlenecks identified:**

[Discussion of limiting factors. Example: "Primary bottleneck is the 4-worker limit, preventing more than 4 tasks from executing concurrently. Secondary bottleneck is the shared filesystem, where concurrent writes to intermediate files may cause contention."]

**Comparison with theoretical maximum:**

[Analysis comparing observed vs. theoretical performance. Example: "With 4 workers and M=8, theoretical maximum speedup is 4x over single-worker execution. Observed speedup is 3.2x, representing 80% efficiency. The 20% overhead comes from task scheduling, data partitioning, and file I/O."]

### 3.3 System Limitations

**Single-machine deployment:**

[Discussion of architectural limitations. Example: "The current implementation uses Docker volume for shared storage, limiting deployment to a single physical machine. Production MapReduce systems like Hadoop use distributed filesystems (HDFS) to scale across multiple machines."]

**Python performance:**

[Discussion of language choice impact. Example: "Python implementation is 10-100x slower than compiled languages like Go or C++. However, for datasets under 1GB, absolute performance is acceptable (jobs complete in seconds), and educational clarity outweighs performance concerns."]

**Simplified fault tolerance:**

[Discussion of reliability. Example: "Current implementation retries failed tasks but doesn't handle worker crashes gracefully. Production systems implement heartbeat monitoring, task backup execution, and worker recovery."]

## 4. Conclusions

### Key Takeaways

1. **Combiner optimization is highly effective**: Reduced intermediate data by [X%] and execution time by [Y%] for word count workload.

2. **Optimal parallelism achieved**: M=8 (2x workers) provides best performance for 4-worker system.

3. **Scalability validated**: System scales efficiently up to worker count limit, with [X%] parallel efficiency.

4. **Design patterns confirmed**: MapReduce paradigm successfully demonstrated for embarrassingly parallel problems.

### Recommendations for Users

**When to use combiner:**

- Reduce function is commutative and associative (e.g., sum, max, count)
- Data has local structure (repeated keys within partitions)
- Network or disk I/O is bottleneck

**Configuration guidelines:**

- Set M to 2-4x number of workers for optimal parallelism
- Set R to 1-2x number of workers to balance reduce phase
- Use larger M for smaller input splits and better load balancing
- Monitor intermediate data size; if >10x output size, investigate optimization opportunities

**Performance tuning:**

1. Enable combiner when applicable
2. Adjust M based on input size and desired granularity
3. Profile map vs. reduce phase duration to identify bottlenecks
4. Consider data preprocessing to improve locality

### Future Work

**Potential improvements:**

1. **Distributed storage**: Replace Docker volume with HDFS or S3 for multi-machine deployment
2. **Advanced scheduling**: Implement locality-aware task scheduling to reduce data transfer
3. **Speculative execution**: Launch backup tasks for stragglers to improve tail latency
4. **Memory optimization**: Use in-memory shuffling instead of intermediate files
5. **Language optimization**: Rewrite performance-critical paths in Go or C++

**Extended evaluation:**

1. Test with larger datasets (10GB+) to stress system limits
2. Evaluate fault tolerance by simulating worker failures
3. Compare with production systems (Hadoop, Spark) for performance benchmarking
4. Measure network bandwidth utilization and I/O patterns

## 5. References

1. Dean, J., & Ghemawat, S. (2004). MapReduce: Simplified Data Processing on Large Clusters. OSDI.
2. [Design Document](design.md): Detailed architecture and implementation
3. [Examples README](examples/README.md): Example jobs and usage guide
4. [Testing README](tests/README.md): Test suite documentation

## Appendix A: Experimental Data

### Raw Measurements

[Include raw data tables with all measurements, including individual run results and standard deviations.]

### Visualization Code

Performance charts generated using `tools/visualize_performance.py`.

### Reproducibility

To reproduce these results:

```bash
# Setup
docker-compose up -d
./examples/setup_examples.sh

# Experiment 1: Combiner effectiveness
python3 tools/compare_performance.py wordcount examples/wordcount.py /mapreduce-data/inputs/shakespeare.txt

# Experiment 2: Scalability
for M in 2 4 8 16 32; do
  python3 client/client.py submit-job \
    --input /mapreduce-data/inputs/shakespeare.txt \
    --output /mapreduce-data/outputs/scale-$M \
    --job-file examples/wordcount.py \
    --num-map-tasks $M \
    --num-reduce-tasks 4 \
    --use-combiner \
    --job-id scale-$M
done

# Generate visualizations
python3 tools/visualize_performance.py
```

## Appendix B: System Specifications

[Detailed system specifications used for experiments, including exact CPU model, disk type, network configuration, etc.]

## Appendix C: Glossary

- **M**: Number of map tasks
- **R**: Number of reduce tasks
- **Combiner**: Local aggregation function executed after map phase
- **Intermediate data**: Output from map tasks, input to reduce tasks
- **Partition**: Subset of keys assigned to a specific reduce task
- **Task**: Unit of work assigned to a worker (either map or reduce)
- **Throughput**: Records or bytes processed per second
- **Speedup**: Ratio of execution time (baseline / optimized)
- **Parallel efficiency**: Speedup / ideal_speedup (as percentage)
