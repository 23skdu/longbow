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

Longbow is designed as a high-performance vector database with modular subsystems:

- **`internal/store`**: The core vector store engine. Handles datasets, indices, and the Search/Put lifecycles.
- **`internal/simd`**: Accelerated vector kernels (AVX-512, Neon) for distance computations and quantization.
- **`internal/gpu`**: Hardware-specific backends for CUDA and Metal acceleration.
- **`internal/store/learned_index.go`**: The **Adaptive Learned Index** system. Uses a k-NN classifier to dynamically select and migrate between HNSW, IVF-PQ, and DiskANN based on real-time performance data.
- **`internal/onnx` & `internal/wazero`**: ML inference backends for embeddings and reranking.

## Development Workflow

### Code Organization

- `cmd/` - Main entry points for the longbow server and CLI tools.
- `internal/` - Private core implementation (Store, SIMD, GPU, ML).
- `pkg/` - Public APIs and SDK components.
- `docs/` - Architectural documentation and user guides.
- `scripts/` - Development utilities, benchmarks, and release automation.

### Development Commands

Use the provided development utilities in `scripts/dev/dev.sh`:

```bash
# Start in development mode with hot reload
./scripts/dev/dev.sh start --dev

# Check status
./scripts/dev/dev.sh status

# View logs
./scripts/dev/dev.sh logs
```

### Hot Reload

The development server supports hot reload via `scripts/dev/dev.sh`:

- Automatic file watching for Go files.
- Graceful restart to preserve in-memory datasets where possible.

### Testing

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
> Heap profiling under high memory pressure (>90% allocation) can cause system instability. Use the `scripts/dev/profile.sh` utility which includes safety checks.

### Prometheus Metrics

Development builds expose metrics on port `:9091/metrics`. Key metrics to watch:

- `longbow_learned_index_training_samples_total`: Training pool size.
- `longbow_learned_index_predictions_total`: prediction distribution (k-NN vs default).
- `longbow_store_memory_usage_bytes`: Real-time memory footprint.

## Getting Help

- **Documentation**: See [docs/](file:///docs/) for deep dives into specific subsystems.
- **Issues**: Search the [GitHub Issues](https://github.com/23skdu/longbow/issues) for known bugs or feature requests.
- **Architecture**: For Agent Memory specific patterns, see [docs/agentmemory.md](file:///docs/agentmemory.md).
