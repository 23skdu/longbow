# Longbow Features (2026)

**Last Updated**: 2026-04-20

---

## Implemented Features (0.1.9-rc3)

### 🚀 Performance & Scalability
- **High-Throughput Parquet IO**: Reflection-free Arrow-to-Parquet encoding using `io.ReaderAt` compatible buffers for multi-threaded snapshotting.
- **Adaptive Indexing**: Automated, zero-downtime migration from flat (linear) scan to HNSW indexing based on collection growth metrics and automated worker-pool lifecycle management.
- **Advanced Graph Structures**: Integrated support for Scalar Quantization (SQ8) and Binary Quantization (BQ) directly within the HNSW metadata layer for ultra-low memory footprints.

### 🧠 Unified ML Inference Engine
- **Cross-Platform WASM Runner (Wazero)**: Full integration of the `wazero` runtime for cross-platform execution of Transformer-based models (embeddings and rerankers) without local library dependencies.
- **Production-Grade Reranking**: Implementation of the `Cross-Encoder` strategy using subword WordPiece tokenization and normalized Transformer scoring.
- **Memory-Efficient Tokenization**: Zero-copy token storage and pooled transformer context management for high-concurrency inference.

### 🔒 Security & Reliability
- **Hardened CGO Bridge**: Remediated 14 high-confidence security findings in SIMD/GPU layers.
- **Audited Subprocess Isolation**: Subprocess execution for GPU discovery is now fully audited and hardened.
- **Comprehensive Test Coverage**: Achieved ~53% total project coverage (~85% in ML packages, ~60% in Query Engine) including rigorous race-condition validation and `-race` detected stability.

### 🔍 Advanced Query Capabilities
- **Multi-Type Filter Evaluator**: Native SIMD-accelerated support for Int32, Uint64, Float64, and String comparisons in metadata filtering.
- **Sharding Result Aggregator**: High-performance result merging and sorting across distributed shards with memory-pooled buffers.
- **SQL Window Functions**: Analytical functions (`ROW_NUMBER`, `RANK`, etc.) and `PARTITION BY` / `ORDER BY` support in TicketQuery.

### 🛠️ Portability & Infrastructure
- **Darwin Core Awareness**: Mach-level processor cluster identification for Apple Silicon (macOS) for core-type-aware worker affinity.
- **Formalized Maintenance Scheduler**: Automated background repair, tombstone reclamation, and memory-limit enforcement tasks.
- **Zero-Copy Network-to-GPU**: libibverbs CGO bindings for Linux/RoCEv2 and RDMA-aware Arrow Flight handshake.

### 📊 Monitoring & Observability
- **Full Prometheus Instrumentation**: Hardware-level metrics including GPU utilization, memory bandwidth, and WASM runtime latency profiles.
- **Distributed Tracing**: End-to-end tracing for the ONNX inference pipeline across distributed sharding boundaries.