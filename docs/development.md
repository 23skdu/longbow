# Development Guide

This document provides guidance for contributing to and developing Longbow.

## Getting Started

### Prerequisites

- **Go 1.24.x or later**: Longbow uses the latest Go features for performance and safety.
- **Git**: For version control.
- **Docker**: For multi-platform builds and deployment testing.
- **Make**: For running build and test targets.
- **Python 3.10+**: Required for running the unified benchmark suite and analysis tools.
- **Hardware Backends (Optional)**:
  - **Metal**: Required for GPU acceleration on macOS (Apple Silicon).
  - **CUDA 12.6+**: Required for NVIDIA GPU acceleration on Linux.

### Setup Development Environment

1. **Clone the repository**

   ```bash
   git clone https://github.com/23skdu/longbow.git
   cd longbow
   ```

2. **Install dependencies**

   ```bash
   make deps
   ```

3. **Install Python benchmark dependencies**

   ```bash
   pip install -r scripts/requirements.txt
   ```

4. **Run tests to verify setup**

   ```bash
   make test
   ```

## Architecture & Subsystems

Longbow is designed as a high-performance vector engine with modular subsystems:

- **`internal/store`**: The core vector store engine. Handles datasets, indices, and the Search/Put lifecycles.
- **`internal/store/internal/core`**: Low-level HNSW, IVF-PQ, and DiskANN index implementations optimized for Arrow memory.
- **`internal/simd`**: Accelerated vector kernels (AVX-512, Neon) for distance computations and quantization.
- **`internal/gpu`**: Hardware-specific backends for CUDA and Metal acceleration.
- **`internal/store/learned_index.go`**: The **Adaptive Learned Index** system. Uses a k-NN classifier to dynamically select and migrate between indices based on real-time performance data.
- **`internal/onnx` & `internal/wasm`**: ML inference backends (using ONNX Runtime and Wazero) for embeddings and reranking.

## Development Workflow

### Code Organization

- `cmd/` - Main entry points for the longbow server and CLI tools.
- `internal/` - Private core implementation (Store, SIMD, GPU, ML, Storage).
- `client/` - Public Go client library.
- `longbowclientsdk/` - Python SDK implementation.
- `docs/` - Architectural documentation and user guides.
- `scripts/` - Development utilities, benchmarks, and release automation.
- `grafana/` - Monitoring dashboards and observability configurations.
- `helm/` - Kubernetes deployment charts.

#### Quality Thresholds

Longbow targets **>95% statement coverage** for all core performance packages (`internal/store`, `internal/simd`, `internal/onnx`).

#### Running Tests

```bash
# Run all tests
make test

# Run tests with race detection (MANDATORY for store changes)
make race

# Run tests with coverage report
make test-coverage

# Run specific test
go test -v ./internal/store -run TestVectorStore
```

#### Fuzzing

Critical paths (quantization, predictor logic, Arrow extraction) must include fuzz tests:

```bash
go test -fuzz=FuzzKNNPredict ./internal/store
```

## Performance Benchmarking

Performance is a first-class citizen in Longbow. All major changes should be verified using the **Unified Benchmark Suite**.

### Unified Benchmark Script

The `scripts/unified_benchmark.py` is the standard tool for verifying performance across dimensions and data types.

```bash
# Run standard CPU benchmarks
python3 scripts/unified_benchmark.py --mode cpu

# Run Learned Index adaptation verification
python3 scripts/unified_benchmark.py --mode learned_index
```

The `learned_index` mode performs a 4-stage validation:

1. Cold start (default heuristics).
2. Data accumulation (training sample collection).
3. Adaptation (k-NN prediction and index migration).
4. Stabilization (performance verification of the new index).

### Bench-Tool Reference

The `bench-tool` (also referred to as `benchmark-tool`) is a high-performance benchmarking utility designed to stress-test Longbow's ingestion and search capabilities across a wide range of data types and configurations. It is a standalone client driver written in Go using the `apache/arrow-go` library and the `SmartClient` SDK, ensuring minimal overhead and maximum throughput accuracy.

#### Key Goals

- **Performance**: Remove Python allocation and dynamic typing overheads for small-batch throughput accuracy.
- **Type Support**: Support a comprehensive scalar type matrix (`float32`, `float64`, `float16`, `int8`, `uint8`, `int16`, `uint16`, `int32`, `uint32`, `int64`, `uint64`, `complex64`, `complex128`) with exact memory layout verification.
- **Full Coverage**: Test all search modalities including Vector, Hybrid, Filtered, Sparse, Geo, Temporal, GraphRAG, and Learned Index paths.

#### Installation

```bash
go build -o bin/bench-tool ./cmd/bench-tool
```

#### Usage

```bash
./bin/bench-tool [options]
```

#### Options

| Flag | Type | Description | Default |
| :--- | :--- | :--- | :--- |
| `-uri` | string | Longbow server URI (supports `grpc://`) | `127.0.0.1:3000` |
| `-dataset` | string | Base name for the benchmark dataset | `bench_go` |
| `-dim` | int | Vector dimensions (max: 3072) | `128` |
| `-scale` | int | Total number of vectors to ingest | `1000` |
| `-dtype` | string | Data type (see "Key Goals" for list) | `float32` |
| `-queries` | int | Number of queries to run for each search mode | `1000` |
| `-workers` | int | Number of concurrent search workers | `1` |
| `-drop` | bool | Automatically drop the dataset after completion | `false` |
| `-json` | string | Path to save benchmark results as JSON | `""` |
| `-tq-bits` | int | Quantization bits for TurboQuant (2, 4, 8) | `4` |
| `-fbin` | string | Path to Arrow IPC binary file for ingestion | `""` |

#### Benchmark Suite Coverage

For every dataset, `bench-tool` executes the following operations in sequence:

1. **DoPut**: Measures bulk ingestion throughput (vec/s and MB/s).
2. **Indexing Wait**: Polls the server until background HNSW/Learned Index construction is complete.
3. **DoGet**: Measures raw retrieval performance.
4. **Search Dense**: Standard HNSW vector search.
5. **Search Hybrid**: Combined vector and text search (BM25 + HNSW).
6. **Search Filtered**: Vector search with complex boolean filters (Arrow-based).
7. **Search Sparse**: Pure text/keyword search.
8. **Search ByID**: Primary key lookups.
9. **Search GraphRAG**: Multi-hop graph expansion + vector similarity.
10. **Search Geo**: Geospatial radius search.
11. **Search Temporal**: Point-in-time and range-based temporal queries.
12. **Search LearnedIndex**: Experimental learned index traversal validation.

#### Examples

**Random Generation Benchmark**

```bash
# Benchmark 100k vectors of float32 in 128 dimensions
./bin/bench-tool -dataset test_rand -scale 100000 -dtype float32 -dim 128 -workers 8
```

**Binary File Ingestion**

```bash
# Benchmark vectors from an Arrow IPC binary file
./bin/bench-tool -dataset test_fbin -fbin data.fbin -queries 5000 -workers 16
```

**TurboQuant Stress Test**

```bash
# Test 1M vectors with 2-bit TurboQuant
./bin/bench-tool -scale 1000000 -dtype turboquant -tq-bits 2 -dim 384 -drop
```

#### Batch Orchestration

For large-scale matrix testing, use the provided scripts:

- `scripts/cli_benchmark.py`: Runs fully isolated blackbox CLI testing to comprehensively exercise the tool's bounds.
- `scripts/unified_benchmark.py`: Python orchestrator for multi-host, multi-architecture (CPU/Metal/CUDA) validation.

#### Output and Monitoring

The tool provides real-time progress for ingestion and indexing. Final results include:

- **Throughput**: Vectors per second and MB per second.
- **Latency**: P50, P95, and P99 latencies in milliseconds.
- **Accuracy**: Recall metrics for HNSW and Learned Index paths (if ground truth available).
- **Stability**: Success/failure counts for each query type.

### Regression Test Plan

This section details the test plan for running comprehensive regression benchmarks of the Longbow vector database at 10,000 and 50,000 vector scale across all data types, dimensions, and query modes.

#### Objectives

- Measure ingest throughput (DoPut) per dtype x dim x count
- Measure search QPS and latency (P50/P95/P99) for all 13 search modes
- Validate all 17 data types at dims 128 and 384 within 16 GB memory budget
- Identify regressions vs previous runs
- Detect SIMD dispatch gaps (integer types, complex types)
- Verify server startup reliability across repeated config cycling

#### Test Configuration

| Parameter | Value |
|-----------|-------|
| Dimensions | 128, 384 |
| Vector counts | 10,000, 50,000 |
| Data types | float32, float64, float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant (4-bit), turboquant2 (2-bit), turboquant4 (4-bit), turboquant8 (8-bit) |
| Search queries | 500 per config |
| Search modes | all (13 modes: dense, hybrid, filtered, filteredbool, filteredstring, sparse, byid, graphrag, globalgraphrag, recommend, geo, temporal, learnedindex) |
| Memory limit | 16 GB (`LONGBOW_MAX_MEMORY=17179869184`) |
| Disk spillover | Auto-spill enabled at 60% threshold (`LONGBOW_AUTO_SPILL_DISK=true`, `LONGBOW_SPILL_THRESHOLD_RATIO=0.60`) for 500k runs |
| Workers | 8 |
| Mode | CPU |
| Total configs | 17 dtypes x 2 dims x 2 counts = **68** |

#### System Requirements

| Resource | Value |
|----------|-------|
| CPU | 16 cores (AVX2) |
| RAM | 22 GB total, 16 GB allocated |
| Storage | 50 GB free |
| OS | Linux x86_64 |
| Go toolchain | 1.22+ (for rebuilding if needed) |

#### Disk Spillover Modes

| Mode | Env Var | Behavior | Use Case |
|------|---------|----------|----------|
| **Auto-spill** (recommended) | `LONGBOW_AUTO_SPILL_DISK=true` | Vectors stay in-memory during HNSW indexing. When memory exceeds the threshold ratio (default 70%, configurable via `LONGBOW_SPILL_THRESHOLD_RATIO`), vectors spill to disk. Graph construction always uses in-memory vectors. | All benchmarks. Fast indexing, prevents OOM at scale. |
| **Forced disk** | `LONGBOW_USE_DISK=1` | Every vector read (including during HNSW graph construction) goes through disk. | **Do not use for benchmarks.** Makes HNSW indexing 10-100x slower because each distance computation requires a disk seek. Only useful for datasets that exceed total RAM + swap. |
| **Disabled** | `LONGBOW_AUTO_SPILL_DISK=false` | All data stays in memory. OOM if insufficient RAM. | Small datasets (<50k vectors) that fit comfortably in RAM. |

**Why forced disk mode breaks benchmarks:** HNSW graph construction at scale requires O(N * ef_construction) distance computations, each needing random access to vector data. With `LONGBOW_USE_DISK=1`, every one of these reads hits disk, turning a 5-minute build into a 60+ minute crawl. Auto-spill avoids this by building the graph in-memory first.

#### Execution Steps

**Phase 1: Cleanup**

```bash
pkill -9 longbow bench-tool
rm -rf data/bench/* data/perf_logs/* profiles/*
mkdir -p data/bench data/perf_logs profiles
```

**Phase 2: Build (if binaries need updates)**

```bash
go build -o bin/longbow -ldflags "-s -w" ./cmd/longbow
go build -o bin/bench-tool -ldflags "-s -w" ./cmd/bench-tool
```

**Phase 3: Run**

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

**Phase 4: Monitoring**

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

**Phase 5: Report Generation**

Results are auto-saved to `data/perf_logs/perf_matrix_*.json` with an accompanying `*.md` report. Copy to docs:

```bash
cp data/perf_logs/perf_matrix_cpu_regression_*.md docs/performance.md
```

#### Output Artifacts

| Artifact | Location | Contents |
|----------|----------|----------|
| JSON results | `data/perf_logs/result_*.json` | Per-config structured results |
| Server logs | `data/perf_logs/longbow_cpu_*.log` | Server diagnostics |
| Bench logs | `data/perf_logs/bench_cpu_*.log` | Bench-tool output |
| Perf matrix | `data/perf_logs/perf_matrix_*.json` | Aggregated all configs |
| Performance doc | `docs/performance.md` | Analysis and findings |
| Next steps | `docs/roadmap.md` | Optimization recommendations |

#### Pass/Fail Criteria

Each config passes if:
- Server starts and stays up through all phases
- All vectors indexed without error
- All search modes return non-zero QPS
- No goroutine/memory panics in logs
- No kernel OOM kill

Full run passes if:
- >=95% of configs complete
- No regressions vs previous runs for comparable configs
- All 13 search modes verified working

#### Results Summary (2026-06-17)

| Metric | Value |
|--------|-------|
| Total configs | 68 |
| Completed | 67 (98.5%) |
| Failed | 1 (int32 dim=128 count=10k -- transient port issue) |
| Duration | ~30 minutes |
| Peak memory | ~9.6 GB (well within 16 GB limit) |
| Regressions | None detected |
| New findings | Integer SIMD dispatch gap for int16/32/64 and uint16/32/64 |

## Contributing

### How to Contribute

1. **Fork the repository** and create a feature branch.
2. **Implement changes** following the [Architecture Guide](architecture.md).
3. **Close the Loop**: If you modify search behavior, ensure you update `RecordQueryPerformance` calls to provide accurate signals for the Learned Index.
4. **Add Tests**: Include unit, integration, and (if applicable) fuzz tests.
5. **Verify Performance**: Run `make benchmark` and Attach the results to your PR.
6. **Submit a Pull Request**.

### Guidelines for the Learned Index

When adding new index types or search optimizations:

- Update `QueryFeatures` if new signals are needed for selection.
- Update `IndexPerformancePredictor` weights if the new component drastically changes the performance landscape.
- Add failure decomposition signals if the component has specific failure modes (e.g., high memory pressure).

## Debugging & Observability

### Profiling

Longbow exposes pprof endpoints at `:9090`:

- CPU Profile: `http://localhost:9090/debug/pprof/profile`
- Heap Profile: `http://localhost:9090/debug/pprof/heap`

> [!CAUTION]
> Heap profiling under high memory pressure (>90% allocation) can cause system instability. Use the `unified_benchmark.py --pprof` utility flag which includes safety checks and auto-backoff.

### Prometheus Metrics

Development builds expose metrics on port `:9091/metrics`. Key metrics to watch:

- `longbow_learned_index_training_samples_total`: Training pool size.
- `longbow_learned_index_predictions_total`: prediction distribution (k-NN vs default).
- `longbow_store_memory_usage_bytes`: Real-time memory footprint.

## Getting Help

- **Documentation**: See [docs/](file:///docs/) for deep dives into specific subsystems.
- **Issues**: Search the [GitHub Issues](https://github.com/23skdu/longbow/issues) for known bugs or feature requests.
- **Architecture**: For Agent Memory specific patterns, see [docs/agentmemory.md](file:///docs/agentmemory.md).
