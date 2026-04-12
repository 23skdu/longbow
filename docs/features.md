# Longbow Features (2026)

**Last Updated**: 2026-04-12

---

## Implemented Features

### Filter Optimization (AVX2/AVX-512)

- SIMD kernels for int64/float64 comparisons in `internal/query/simd_filter_amd64.go`
- Bitwise AND/OR kernels for mask merging in `internal/simd/bitops_amd64.go`
- AVX-512 k-mask operations for compatible hardware
- Early Exit logic based on bitpool density

### Multi-GPU Aggregates

- Parallel search/filter dispatch via `internal/gpu/multi_gpu.go`
- Heap-based merge for distributed Top-K results
- SUM/AVG/MIN/MAX across GPU shards

### SQL Window Functions

- `WindowOperator` in `internal/query/window_operator.go`
- Analytical functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`, `AVG`, `MIN`, `MAX`
- `PARTITION BY` and `ORDER BY` support in TicketQuery
- Integrated into Search pipeline with dynamic Arrow columns

### Hardware-Accelerated PQ (VNNI/GPU)

- AVX-512 VNNI lookup kernels
- CUDA/Metal kernels for batch PQ compression
- ADC (Asymmetric Distance Computation) table builders

### Dataset Import/Export

- `dataset_io.go` with Parquet Export/Import
- Arrow Record to Parquet stream conversion
- ExportDataset/ImportDataset wired to StorageBackend

### ONNX Runtime Integration

- Unified ONNX bridge for Metal (macOS) and ONNX Runtime (Linux/CUDA)
- Functional reranker and embedding generator
- Graceful fallbacks for non-GPU environments

### CUDA Memory Operations

- `allocateCUDAMemory` and `freeCUDAMemory`
- `cudaMemcpyHostToDevice` and `DeviceToHost`
- Unified memory management in `GPUMemPool`

### Vectorized Metadata Filtering

- SIMD comparison kernels for Arrow types
- Bitmask merging using AVX-512 k-mask registers
- Early Exit logic for index traversal

### Monitoring & Observability

- GPU utilization and memory metrics (`internal/metrics/gpu_metrics.go`)
- Tracing for ONNX inference pipelines
- Grafana dashboards for GPU/ONNX health (`grafana/dashboards/gpu-onnx-health.json`)

### Zero-Copy Network-to-GPU

- libibverbs CGO bindings for Linux/RoCEv2
- RDMA-aware Arrow Flight handshake
- Fallback stubs for non-IB environments