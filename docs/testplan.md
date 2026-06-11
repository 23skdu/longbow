# Test Plan - Benchmarking and Performance Evaluation (500k Scale)

This document details the test plan for performing a complete benchmarking run of the Longbow vector database at a 500k vector scale across all data types, dimensions, and query modes.

## Proposed Steps

### 1. Cleanup Phase
- Stop any existing longbow or bench-tool processes.
- Clean up old compiled binaries:
  - `bin/longbow`
  - `bin/bench-tool`
  - `bin/longbow-cli`
  - `bench-tool` (root)
  - `index.test` (root)
- Clean up old profile files:
  - `profiles/*.pprof`
- Clean up old performance logs and WAL files:
  - `data/wal.log`
  - `data/perf_logs/*`

### 2. Build Phase
- Build Go binaries:
  - `go build -o bin/longbow ./cmd/longbow`
  - `go build -o bin/bench-tool ./cmd/bench-tool`
  - `go build -o bin/longbow-cli ./cmd/cli`

### 3. Execution Phase
- Run the unified benchmark runner `scripts/unified_benchmark.py` in the background with the following options:
  - `--mode cpu`
  - `--dims 128,384`
  - `--dtypes uint8,uint16,uint32,uint64,int8,int16,int32,int64,float16,float32,float64,complex64,complex128,turboquant2,turboquant4,turboquant8`
  - `--counts 500000`
  - `--search-modes dense,sparse,hybrid,temporal,graphrag`
  - `--queries 10`
  - `--memory 17179869184` (16GB)
  - `--pprof`

### 4. Progress Monitoring & 10-Min Reporting
- Execute the python command as a background task.
- Schedule a recurring alarm (every 10 minutes / 600 seconds) to check on execution progress:
  - Tail the stdout/stderr log of the running benchmark.
  - Record current progress (running configuration, elapsed time, completed runs).
  - Print status updates.

### 5. Report Generation & Recommendations
- Extract the generated markdown report from `data/perf_logs/perf_matrix_*.md` and copy it to `docs/performance.md`.
- Analyze performance bottlenecks and add optimization recommendations to `docs/nextsteps.md`.
