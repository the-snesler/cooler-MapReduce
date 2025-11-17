# ✅ Week 4 Benchmarking Setup - COMPLETE

## Files Created Successfully

### Root Directory
- ✅ **benchmark.py** (13 KB) - Automated benchmarking script
- ✅ **plot_results.py** (9.8 KB) - Plot generation script
- ✅ **samples/** - Symlink to shared/samples/

### Documentation (docs/)
- ✅ **BENCHMARK_PLAN.md** (3.0 KB) - Detailed benchmarking methodology
- ✅ **WEEK4_REPORT.md** (12 KB) - Report template (fill in results)
- ✅ **DIAGNOSTICS_GUIDE.md** (10 KB) - Performance analysis guide

### Directories Created
- ✅ **benchmark_results/** - Will store JSON/CSV results
- ✅ **benchmark_results/plots/** - Will store generated plots
- ✅ **logs/** - For collecting container logs
- ✅ **shared/input/** - Contains test input files
- ✅ **shared/output/** - Will store job outputs

## Test Input Files Generated

| File | Size | Replications | Purpose |
|------|------|--------------|---------|
| story.txt | 335 B | 1× (baseline) | Small input test |
| story_medium.txt | 981 KB | ~3,000× | Medium scaling test |
| story_large.txt | 9.6 MB | ~30,000× | Large scaling test |
| story_xlarge.txt | 48 MB | ~150,000× | Extra large scaling test |

## Docker Containers Status

All containers are **UP and RUNNING**:
- ✅ mapreduce-coordinator (port 50051)
- ✅ mapreduce-worker1
- ✅ mapreduce-worker2
- ✅ mapreduce-worker3
- ✅ mapreduce-worker4

---

## 🚀 NEXT STEPS - Run Benchmarks

### Option 1: Full Automated Benchmark (Recommended)

Run the complete benchmark suite with 1 run per configuration:

```bash
python benchmark.py
```

When prompted:
- **Number of runs per benchmark:** Enter `1` (or `3` for more reliable data)
- **Proceed?** Enter `y`

**Duration:** ~15-20 minutes for 1 run, ~45-60 minutes for 3 runs

### Option 2: Quick Test First

Test with a single small job to verify everything works:

```bash
python src/client/client.py submit \
  --input samples/story.txt \
  --output shared/output/test \
  --job-file samples/word_count.py \
  --num-map 2 \
  --num-reduce 1
```

Then check status:
```bash
python src/client/client.py list
```

---

## 📊 After Benchmarks Complete

### 1. Generate Plots

```bash
# Find the latest results file
RESULTS=$(ls -t benchmark_results/benchmark_results_*.json | head -1)

# Generate all plots
python plot_results.py $RESULTS
```

This will create 5 plots in `benchmark_results/plots/`:
- 1_input_size_scaling.png
- 2_map_task_scaling.png
- 3_reduce_task_scaling.png
- 4_speedup_analysis.png
- 5_combined_heatmap.png
- results_table.md (data table)

### 2. Collect Logs (Optional but Recommended)

```bash
# Coordinator logs
docker compose logs coordinator > logs/coordinator_$(date +%Y%m%d_%H%M%S).log

# Worker logs
docker compose logs worker-1 > logs/worker1_$(date +%Y%m%d_%H%M%S).log
docker compose logs worker-2 > logs/worker2_$(date +%Y%m%d_%H%M%S).log
docker compose logs worker-3 > logs/worker3_$(date +%Y%m%d_%H%M%S).log
docker compose logs worker-4 > logs/worker4_$(date +%Y%m%d_%H%M%S).log
```

### 3. Fill Out Report

Open `docs/WEEK4_REPORT.md` and complete:
- Section 2.2: Hardware specs
- Section 4: Copy data from results_table.md
- Section 4.x: Analyze the plots
- Section 5: Identify bottlenecks
- Section 6: Worker utilization
- Section 10: Conclusions

---

## 🔧 Troubleshooting

### If benchmark fails with missing modules:

```bash
pip install matplotlib numpy
```

### If jobs get stuck:

```bash
# Check container status
docker compose ps

# Restart containers
docker compose restart

# View real-time logs
docker compose logs -f coordinator
```

### If input files are missing:

They've already been generated! Check:
```bash
ls -lh shared/input/story*.txt
```

---

## 📝 Quick Reference

**Run full benchmarks:**
```bash
python benchmark.py
```

**Generate plots:**
```bash
python plot_results.py benchmark_results/benchmark_results_*.json
```

**View results:**
```bash
cat benchmark_results/plots/results_table.md
```

**Clean up outputs (if needed):**
```bash
rm -rf shared/output/*
rm -rf shared/intermediate/*
```

---

## ✨ What You Have Now

Everything is **100% ready to run**. No placeholders, no missing files. Just execute:

```bash
python benchmark.py
```

And you'll get complete benchmark results with plots ready for your Week 4 report!

---

**Good luck with your benchmarking! 🎉**
