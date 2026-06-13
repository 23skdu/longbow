# Test Plan — Benchmarking and Performance Evaluation (1M Scale)

This document details the test plan for running comprehensive benchmarks of the Longbow vector database at 1,000,000 vector scale across all data types, dimensions, and query modes.

---

## 1. Objectives

- Measure ingest throughput (DoPut, DoGet) per dtype × dim
- Measure HNSW indexing time at 1M scale
- Measure search QPS and latency (P50/P95/P99) for all 13 search modes
- Collect pprof profiles (cpu, heap, allocs, goroutine, threadcreate, block, mutex) at start and end of each test
- Identify memory bottlenecks and stability issues at scale
- Validate the LockFreeNeighborCache deadlock fix under thread-pinned parallel insert

---

## 2. System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 8 cores | 16 cores (AVX2) |
| RAM | 16 GB | 32 GB |
| Storage | 50 GB free | 100 GB free (for logs + profiles) |
| OS | Linux x86_64 | Linux x86_64 with AVX2 |
| Go | 1.22+ | 1.23+ |

Memory budget by dtype at 1M dim384 (estimated):
- float32/int8/uint8/turboquant2: ~14 GB
- float64/int32/uint32/turboquant4: ~18 GB
- int64/uint64/complex64/turboquant8: ~22 GB
- complex128: ~28 GB

## 3. Test Configurations

### Dimensions
- 128, 384

### Data Types (17 total)
- `float32`, `float64`, `float16`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `complex64`, `complex128`
- `turboquant`, `turboquant2`, `turboquant4`, `turboquant8`

### Vector Counts
- 1,000,000 (default)
- Reduce to 500,000 for large types that OOM at 1M

### Search Modes (13 total)
- `dense`, `hybrid`, `filtered`, `filteredbool`, `filteredstring`
- `sparse`, `byid`
- `graphrag`, `globalgraphrag`, `recommend`
- `geo`, `temporal`
- `learnedindex`

## 4. Execution Steps

### Phase 1: Cleanup

```bash
# Kill all longbow/bench-tool processes
pkill -9 longbow bench-tool

# Remove old data
rm -rf data/bench data/perf_logs profiles/*.pprof

# Recreate directories
mkdir -p data/perf_logs profiles
```

### Phase 2: Build

```bash
go build -o bin/longbow -ldflags "-s -w" ./cmd/longbow
go build -o bin/bench-tool -ldflags "-s -w" ./cmd/bench-tool
```

### Phase 3: Run Benchmark

**Full run (all 17 types, both dims):**

```bash
LONGBOW_MAX_MEMORY=17179869184 PYTHONUNBUFFERED=1 \
python3 scripts/unified_benchmark.py \
  --mode cpu \
  --dims 128,384 \
  --counts 1000000 \
  --dtypes float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant,turboquant2,turboquant4,turboquant8 \
  --queries 1000 \
  --pprof \
  --label full_bench \
  --memory 17179869184 \
  --timeout 7200 > /tmp/bench_full.log 2>&1 &
```

**Batched approach (if memory-constrained):**

Batch A (small):
```bash
--dtypes float32,float16,int8,uint8,turboquant2
```

Batch B (medium):
```bash
--dtypes float64,int16,uint16,int32,uint32,turboquant4
```

Batch C (large, >16 GB risk):
```bash
--dtypes int64,uint64,complex64,complex128,turboquant,turboquant8
# Use --counts 500000 for large dim384 dtypes
```

### Phase 4: Monitoring

Check every 10 minutes:

```bash
# Number of result JSONs generated
ls -1 data/perf_logs/result_*.json | wc -l

# Current configuration in progress
tail -5 /tmp/bench_full.log

# Memory status
free -h

# Check for OOM
dmesg | grep -i "killed process" || journalctl -xn 20 | grep -i oom

# Process health
ps aux | grep -E "longbow|bench-tool"
```

### Phase 5: Report Generation

After completion, parse results and update docs:

```bash
# Parse the perf matrix
python3 scripts/parse_results.py data/perf_logs/perf_matrix_*.json

# Copy reports
cp data/perf_logs/perf_matrix_*.md docs/performance.md
```

## 5. Output Artifacts

| Artifact | Location | Contents |
|----------|----------|----------|
| JSON results | `data/perf_logs/result_*.json` | Per-test structured results |
| Server logs | `data/perf_logs/longbow_*.log` | Server diagnostics, memory usage |
| Bench logs | `data/perf_logs/bench_*.log` | Bench-tool output per run |
| pprof profiles | `profiles/*.pprof` | CPU, heap, allocs, goroutine, threadcreate, block, mutex |
| Perf matrix | `data/perf_logs/perf_matrix_*.json` | Aggregated results across all configs |
| Performance doc | `docs/performance.md` | Benchmark summary and analysis |
| Next steps | `docs/nextsteps.md` | Optimization recommendations |

## 6. Pass/Fail Criteria

Each test configuration passes if:
- Server starts successfully and stays up through all phases
- All 1,000,000 vectors are indexed without error
- All 13 search modes return results with non-zero QPS
- No goroutine or memory panics in server or bench logs
- No OOM kill from the kernel

Full benchmark run passes if:
- All non-OOM configurations complete with zero errors
- Coverage: all 17 dtypes × 2 dims = 34 configurations (or as many as fit in 16 GB)

## 7. Lessons Learned (2026-06-12)

### From the 1M Run

- **Deadlock in LockFreeNeighborCache**: The initial run at float32 dim128 deadlocked at ~820K/1M during HNSW build. Fixed by removing lock promotion (`RLock→RUnlock→Lock`) and using direct `Lock`. After fix, all completed runs were clean.

- **Memory limit too tight for large types**: float64 dim384 exceeded 16 GB and was still indexing after 58 min. Larger types (complex128, turboquant8) at dim384 would OOM. Either increase memory to 24 GB+ or reduce test scale for large types.

- **10-min monitoring interval is sufficient**: Each test takes 5-50 min. No need for more frequent checks.

- **pprof overhead is negligible**: The 5-10% overhead from periodic pprof collection is acceptable for benchmark runs.

- **Script doesn't auto-resume on crash**: If a server or bench-tool dies, the Python script hangs indefinitely (waiting on subprocess). Manual intervention is required.

### For Next Run

- Add `--timeout` per bench-tool, not just Python-level timeout
- Use `--counts 500000` for large dtypes at dim384 as a safety valve
- Consider separate runs per memory-profile batch
- Add swap detection to the monitoring checklist
- Use `systemd-cgtop` or similar to track cgroup memory limits
