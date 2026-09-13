# Longbow Features (2026)

Longbow is designed to provide FAISS-level performance with Arrow-native ergonomics and GraphRAG integration.

**Last Updated**: 2026-09-12

---

## Implemented Features

### Performance & Scalability

- **TurboQuant (TQ) V2**: Extended bit-packing to support **2-bit quantization**, achieving up to 64x memory compression for extreme-scale archival indices.
- **Autonomous efSearch Tuning**: Integrated PID-controller for dynamic search depth adjustment, replacing static heuristics with real-time feedback-driven recall targeting.
- **NEON SIMD Parity**: Full metadata filtering acceleration for ARM64 (macOS/Apple Silicon) via hand-optimized NEON assembly kernels.
- **Lock-Free Ingestion Workers**: High-performance ingestion pipeline using `LockFreeRingBuffer` and adaptive batching to eliminate mutex contention.
- **Runtime Learned Index**: `IndexPerformancePredictor` selects the optimal ANN index type per query using a k-NN classifier.
- **Adaptive Flat to HNSW Migration**: Automated, zero-downtime promotion from flat scan to HNSW indexing.
- **GPU Complex Type Kernels**: Native CUDA kernels for complex128/complex64 L2 distance, dot product, and cosine similarity, eliminating CPU fallback for complex vector search.
- **Conditional Math Dispatch**: `LONGBOW_MATH_DISPATCH` env var for selective EMLGo routing by type and scale — emlgo for complex types + TQ above 50k, standard for int/float below 50k.
- **Benchmark Multi-Run**: Statistical aggregation with mean/stdev reporting across multiple benchmark runs, plus memory soak testing for long-duration stability validation.

### Advanced Quantization Suite

- **Product Quantization (PQ)**: Sophisticated sub-vector quantization with optimized codebook training for extreme memory reduction (16x-32x) while maintaining high recall.
- **Scalar Quantization (SQ8)**: Integrated support for 8-bit scalar quantization directly within the HNSW metadata layer for 4x memory reduction.
- **Binary Quantization (BQ)**: Extremely fast Hamming-distance based search for binary-encoded vectors (32x memory reduction).
- **Float16 Support**: Native half-precision vector storage and distance computation for 2x memory savings.

### Specialized Search Capabilities

- **Distributed GraphRAG**: Support for cross-node BFS and activation propagation protocols, enabling traversal of multi-billion node knowledge graphs across clusters.
- **Persistent HNSW Memory Mapping**: Direct Arrow-backed `mmap` for HNSW graph storage, eliminating indexing load times and enabling instant-on cold starts.
- **Geo-Search Engine**: Native support for Haversine distance and geospatial indexing using AVX-accelerated Quadtrees.
- **SQL Analytical Functions**: Full support for `ROW_NUMBER`, `RANK`, and windowing functions.
- **Multi-Type Filter Evaluator**: Native SIMD-accelerated support for Int32, Uint64, Float64, and String comparisons.
- **Automatic Sharding**: Distributed index management splitting large datasets into shards based on growth.

### Unified ML Inference Engine

- **Cross-Platform WASM Runner (Wazero)**: Full integration of the `wazero` runtime for cross-platform execution of Transformer-based models (embeddings and rerankers) without local library dependencies.
- **Production-Grade Reranking**: Implementation of the `Cross-Encoder` strategy using subword WordPiece tokenization and normalized Transformer scoring.
- **Memory-Efficient Tokenization**: Zero-copy token storage and pooled transformer context management for high-concurrency inference.

### Security & Reliability

- **Hardened CGO Bridge**: Remediated all high-confidence security findings in SIMD/GPU layers and removed vulnerable FAISS dependencies.
- **Audited Subprocess Isolation**: Subprocess execution for GPU discovery is now fully audited and hardened.
- **Comprehensive Test Coverage**: Achieved >95% total project coverage including rigorous race-condition validation and `-race` detected stability.

### Portability & Infrastructure

- **Darwin Core Awareness**: Mach-level processor cluster identification for Apple Silicon (macOS) for core-type-aware worker affinity.
- **Formalized Maintenance Scheduler**: Automated background repair, tombstone reclamation, and memory-limit enforcement tasks.
- **Zero-Copy Network-to-GPU**: libibverbs CGO bindings for Linux/RoCEv2 and RDMA-aware Arrow Flight handshake.
- **Python SDK (Zero-Copy)**: High-performance client using Arrow Flight for wire-speed ingestion and retrieval. Features native NumPy/Pandas integration and automatic vector type inference to eliminate serialization overhead.
- **Administrative CLI**: Comprehensive Go-based tool for cluster management, geospatial search, and large-scale S3 Parquet ingestion. Enables terminal-based GraphRAG operations and dataset lifecycle management.

### Monitoring & Observability

- **Full Prometheus Instrumentation**: Hardware-level metrics including GPU utilization, memory bandwidth, and WASM runtime latency profiles.
- **Distributed Tracing**: End-to-end tracing for the ONNX inference pipeline across distributed sharding boundaries.

---

## Competitive Landscape

### Feature Matrix

| Feature | Chroma | Milvus | Qdrant | **FAISS** | Pinecone | TencentDB | **Longbow** |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Focus** | Prototyping | Massive Scale | Speed & Efficiency | **Perf Library** | Managed SaaS | Enterprise SaaS | **Structural Discovery** |
| **Quantization** | No | **RaBitQ / PQ** | PQ/SQ/BQ | **PQ/SQ/OPQ** | Proprietary | PQ / SQ | **IVF-OPQ / TurboQuant (2/4/8-bit)** |
| **Architecture** | SQLite | Distributed | Rust | **C++/CUDA** | Closed/Cloud | Distributed (OLAMA) | **Zero-Copy Arrow** |
| **GPU Support** | CPU-Only | **Tier 1** | **Tier 1** | **Tier 1 (NVIDIA)** | Managed | Managed | **Tier 1 (Metal/CUDA)** |
| **SIMD Optim.** | Library | Extensive | **Native** | **Extensive** | Managed | Extensive | **Custom AVX2/AVX512/NEON** |
| **GraphRAG** | No | Basic | No | No | No | External (Langchain) | **Dual-Path: Spreading + Knowledge Graph** |
| **Temporal** | No | No | No | No | No | No | **Native Versioning (Default On)** |
| **Geo-Spatial** | No | No | Native | No | No | Basic | **Native Quadtree** |

### Performance Parity (0.1.9)

Internal benchmarks on 1M vectors (1536D) show that Longbow is within 5% of FAISS's raw C++ throughput while providing much better memory efficiency via Arrow zero-copy memory management.

| Metric | FAISS (IVF-PQ) | Longbow (IVF-HNSW) | Longbow (TurboQuant) |
| :--- | :--- | :--- | :--- |
| Search Latency (1M) | ~2.5ms | ~1.8ms | ~0.4ms |
| Memory Overhead | 2.5x | 1.8x | 0.25x |
| Build Throughput | 10k/s | 25k/s | 40k/s |
| Compression Ratio | 4x | 1x | 4-64x |
| SIMD Optimization | Partial (AVX2) | Full (AVX-512/AMX) | Full |
| TPU Acceleration | No | Yes | No (Use CPU TQ) |
| Zero-Copy Flight | No | Yes | Yes |

**TurboQuant (0.1.9 New)**: Two-stage vector compression combining Polar Quantization with QJL correction. Achieves 4-64x compression with configurable bit depth (2/4/8 bits per angle).

### Per-Competitor Analysis

#### FAISS

- **GPU**: The industry benchmark for NVIDIA GPU acceleration. Supports massive parallelization and multi-GPU indexing via **IVF-PQ** and **HNSW-Flat**.
- **SIMD**: Highly optimized C++ core utilizing AVX2, AVX-512, and ARM Neon for maximum throughput on dense vector operations.

#### Tencent Cloud VectorDB

- **Architecture**: A fully managed, enterprise-level distributed database powered by the "AI Native" **OLAMA** vector engine, designed to support up to 1 billion vectors in a single index with high availability.
- **Quantization & Indexing**: Employs standard quantization methods like Scalar Quantization (SQ) and Product Quantization (PQ) to balance the memory intensity of its HNSW-backed indexing, though heavy reliance on in-memory indexes can lead to "cold start" latency spikes if not actively warmed.
- **Ecosystem**: Relies on external frameworks (e.g., LangChain, LlamaIndex) and Ollama for LLM runtimes to implement GraphRAG and entity extraction, whereas Longbow provides a native dual-path approach.

#### Longbow

- **Arrow-Native**: No serialization overhead when interacting with Arrow-based data pipelines or DuckDB.
- **GraphRAG**: Dual-path architecture combining:
  - **Spreading Activation**: Vector-based re-ranking using HNSW Layer 0 graph expansion
  - **Knowledge Graph**: Triple-based (SPOW) explicit relationships with PageRank & Community Detection
- **Hybrid Search**: Seamless integration of vector similarity with full-text search (BM25) and metadata filtering.
- **TurboQuant**: Novel two-stage compression achieving extreme density with fast search.

---

### New Features in 0.1.9

#### Quantization

- **IVF-OPQ**: Optimized Product Quantization with iterative training
- **TurboQuant**: 2-bit, 4-bit, and 8-bit compression modes
- **Auto-Tuning**: Automatic selection between float32/int8/PQ/TQ based on memory/recall

#### Performance

- **SIMD**: Complete AVX2, AVX512, and NEON kernels
- **Batching**: DoPut bulk path for >=100 vectors
- **io_uring**: Linux async I/O for WAL operations
- **GPU Complex Kernels**: Native CUDA L2/dot/cosine for complex128/complex64
- **Conditional Dispatch**: EMLGo routing by type and scale via `LONGBOW_MATH_DISPATCH`
- **Benchmark Multi-Run**: Statistical mean/stdev across runs + memory soak tests

#### Search

- **Temporal**: Native versioning (enabled by default)
- **Hybrid**: Vector + BM25 + metadata filtering
- **GraphRAG**: Dual-path (Spreading + Knowledge Graph) with PageRank & Community Detection
- **Complex Vector Search**: First-class complex128/complex64 support across CPU and GPU

#### Quality

- **Fuzz Tests**: IVF index build, TurboQuant encode/decode
- **Metrics**: Prometheus metrics for batching, quantization, SIMD
- **Memory Soak**: Long-duration stability validation for production readiness
