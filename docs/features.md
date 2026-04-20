# Longbow Features (2026)

**Last Updated**: 2026-04-20

---

## Implemented Features

### 🔒 Security & Reliability (0.1.9)
- **Hardened CGO Bridge**: Remediated 14 high-confidence security findings in SIMD/GPU layers.
- **Audited Subprocess Isolation**: Subprocess execution for GPU discovery is now fully audited and hardened.
- **Integer Casting Safety**: Applied safety checks and audited suppressions for integer casts in hashing and sharding.
- **Comprehensive Test Coverage**:
  - **100% ONNX Integration** coverage.
  - **SIMD Suite**: Comprehensive validation of all distance metrics and data types.
  - **Core Lifecyle**: Hardened search context pooling and candidate heap management.

### Production-Grade ML Ingestion
- **WordPiece Tokenization**: Functional subword-accurate text encoding for transformer-based models (replacing skeletal word-length proxies).
- **Wasm Shared Memory Inference**: Cross-platform ML execution via `wazero` with `malloc`/`free` memory sharing for high-throughput tensor passing.
- **Transformer Mean-Pooling**: Integrated hidden-state pooling and L2-normalization for high-fidelity dense vector generation.
- **Dedicated Internal ML Bridge**: Unified encoding and inference pipeline for embeddings and cross-encoder reranking.

### High-Throughput IO & Serialization
- **Streaming Parquet Serialization**: Reflection-free Arrow-to-Parquet encoder using `parquet-go` for zero-copy disk persistence.
- **Asynchronous Disk Submission**: Foundation for `io_uring` integration on Linux, significantly increasing disk IOPS for metadata and vector storage.

### Portability & Infrastructure
- **Darwin Core Awareness**: Mach-level processor cluster identification for Apple Silicon (macOS), enabling core-type-aware worker affinity.
- **Formalized Maintenance Scheduler**: Automated background repair, tombstone reclamation, and memory-limit enforcement tasks.
- **Stabilization Metrics**: Comprehensive Prometheus instrumentation for HNSW repair cycles, search early-exit reasons, and WASM runtimes.

### Filter Optimization (AVX2/AVX-512)
- SIMD kernels for int64/float64 comparisons.
- Bitwise AND/OR kernels for mask merging.
- AVX-512 k-mask operations for compatible hardware.

### Multi-GPU Aggregates
- Parallel search/filter dispatch via `internal/gpu/multi_gpu.go`.
- Heap-based merge for distributed Top-K results.

### SQL Window Functions
- Analytical functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`, `AVG`, `MIN`, `MAX`.
- `PARTITION BY` and `ORDER BY` support in TicketQuery.

### Hardware-Accelerated PQ (VNNI/GPU)
- AVX-512 VNNI lookup kernels.
- CUDA/Metal kernels for batch PQ compression.

### ONNX Runtime Integration
- Unified ONNX bridge for Metal (macOS) and ONNX Runtime (Linux/CUDA).
- Functional reranker and embedding generator.

### Monitoring & Observability
- GPU utilization and memory metrics.
- Tracing for ONNX inference pipelines.
- Grafana dashboards for GPU/ONNX health.

### Zero-Copy Network-to-GPU
- libibverbs CGO bindings for Linux/RoCEv2.
- RDMA-aware Arrow Flight handshake.