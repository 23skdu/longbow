# Test Plan — Regression Benchmark (50k Scale, All Data Types)

This document details the test plan for running comprehensive regression benchmarks of the Longbow vector database at 10,000 and 50,000 vector scale across all data types, dimensions, and query modes.

---

## 1. Objectives

- Measure ingest throughput (DoPut) per dtype × dim × count
- Measure search QPS and latency (P50/P95/P99) for all 13 search modes
- Validate all 17 data types at dims 128 and 384 within 16 GB memory budget
- Identify regressions vs previous runs
- Detect SIMD dispatch gaps (integer types, complex types)
- Verify server startup reliability across repeated config cycling

## 2. Test Configuration

| Parameter | Value |
|-----------|-------|
| Dimensions | 128, 384 |
| Vector counts | 10,000, 50,000 |
| Data types | float32, float64, float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant (4-bit), turboquant2 (2-bit), turboquant4 (4-bit), turboquant8 (8-bit) |
| Search queries | 500 per config |
| Search modes | all (13 modes: dense, hybrid, filtered, filteredbool, filteredstring, sparse, byid, graphrag, globalgraphrag, recommend, geo, temporal, learnedindex) |
| Memory limit | 16 GB (`LONGBOW_MAX_MEMORY=17179869184`) |
| Workers | 8 |
| Mode | CPU |
| Total configs | 17 dtypes × 2 dims × 2 counts = **68** |

## 3. System Requirements

| Resource | Value |
|----------|-------|
| CPU | 16 cores (AVX2) |
| RAM | 22 GB total, 16 GB allocated |
| Storage | 50 GB free |
| OS | Linux x86_64 |
| Go toolchain | 1.22+ (for rebuilding if needed) |

## 4. Execution Steps

### Phase 1: Cleanup

```bash
pkill -9 longbow bench-tool
rm -rf data/bench/* data/perf_logs/* profiles/*
mkdir -p data/bench data/perf_logs profiles
```

### Phase 2: Build (if binaries need updates)

```bash
go build -o bin/longbow -ldflags "-s -w" ./cmd/longbow
go build -o bin/bench-tool -ldflags "-s -w" ./cmd/bench-tool
```

### Phase 3: Run

```bash
export LONGBOW_MAX_MEMORY=17179869184
export LONGBOW_BENCH_FAST=0
export PYTHONUNBUFFERED=1

python3 scripts/unified_benchmark.py \
  --mode cpu \
  --dims 128,384 \
  --counts 10000,50000 \
  --dtypes float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant,turboquant2,turboquant4,turboquant8 \
  --queries 500 \
  --memory 17179869184 \
  --timeout 3600 \
  --label regression \
  --workers 8
```

### Phase 4: Monitoring

Check every 10 minutes:

```bash
# Completed configs
ls -1 data/perf_logs/result_*.json | wc -l

# Current config
tail -5 benchmark_run.log

# Memory pressure
free -h

# OOM check
dmesg | grep -i "killed process"

# Errors
grep -i "error\|fail\|exhausted\|panic\|CRASH" benchmark_run.log
```

### Phase 5: Report Generation

Results are auto-saved to `data/perf_logs/perf_matrix_*.json` with an accompanying `*.md` report. Copy to docs:

```bash
cp data/perf_logs/perf_matrix_cpu_regression_*.md docs/performance.md
```

## 5. Output Artifacts

| Artifact | Location | Contents |
|----------|----------|----------|
| JSON results | `data/perf_logs/result_*.json` | Per-config structured results |
| Server logs | `data/perf_logs/longbow_cpu_*.log` | Server diagnostics |
| Bench logs | `data/perf_logs/bench_cpu_*.log` | Bench-tool output |
| Perf matrix | `data/perf_logs/perf_matrix_*.json` | Aggregated all configs |
| Performance doc | `docs/performance.md` | Analysis and findings |
| Next steps | `docs/nextsteps.md` | Optimization recommendations |

## 6. Pass/Fail Criteria

Each config passes if:
- Server starts and stays up through all phases
- All vectors indexed without error
- All search modes return non-zero QPS
- No goroutine/memory panics in logs
- No kernel OOM kill

Full run passes if:
- ≥95% of configs complete
- No regressions vs previous runs for comparable configs
- All 13 search modes verified working

## 7. Results Summary (2026-06-17)

| Metric | Value |
|--------|-------|
| Total configs | 68 |
| Completed | 67 (98.5%) |
| Failed | 1 (int32 dim=128 count=10k — transient port issue) |
| Duration | ~30 minutes |
| Peak memory | ~9.6 GB (well within 16 GB limit) |
| Regressions | None detected |
| New findings | Integer SIMD dispatch gap for int16/32/64 and uint16/32/64 |
