# Longbow vs. The World

Longbow is designed to provide FAISS-level performance with Arrow-native ergonomics and GraphRAG integration.

| Feature | Chroma | Milvus | Qdrant | **FAISS** | Pinecone | **Longbow** |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Focus** | Prototyping | Massive Scale | Speed & Efficiency | **Perf Library** | Managed SaaS | **Structural Discovery** |
| **Quantization** | No | **RaBitQ / PQ** | PQ/SQ/BQ | **PQ/SQ/OPQ** | Proprietary | **IVF-OPQ / TurboQuant (2/4/8-bit)** |
| **Architecture** | SQLite | Distributed | Rust | **C++/CUDA** | Closed/Cloud | **Zero-Copy Arrow** |
| **GPU Support** | CPU-Only | **Tier 1** | **Tier 1** | **Tier 1 (NVIDIA)** | Managed | **Tier 1 (Metal/CUDA)** |
| **SIMD Optim.** | Library | Extensive | **Native** | **Extensive** | Managed | **Custom AVX2/AVX512/NEON** |
| **GraphRAG** | No | Basic | No | No | No | **Dual-Path: Spreading + Knowledge Graph** |
| **Temporal** | No | No | No | No | No | **Native Versioning (Default On)** |
| **Geo-Spatial** | No | No | Native | No | No | **Native Quadtree** |

---

## Performance Parity (0.1.9)

Our internal benchmarks on 1M vectors (1536D) show that Longbow is within 5% of FAISS's raw C++ throughput while providing much better memory efficiency via Arrow zero-copy memory management.

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

---

## New Features in 0.1.9

### Quantization
- **IVF-OPQ**: Optimized Product Quantization with iterative training
- **TurboQuant**: 2-bit, 4-bit, and 8-bit compression modes
- **Auto-Tuning**: Automatic selection between float32/int8/PQ/TQ based on memory/recall

### Performance
- **SIMD**: Complete AVX2, AVX512, and NEON kernels
- **Batching**: DoPut bulk path for >=100 vectors
- **io_uring**: Linux async I/O for WAL operations

### Search
- **Temporal**: Native versioning (enabled by default)
- **Hybrid**: Vector + BM25 + metadata filtering
- **GraphRAG**: Dual-path (Spreading + Knowledge Graph) with PageRank & Community Detection

### Quality
- **Fuzz Tests**: IVF index build, TurboQuant encode/decode
- **Metrics**: Prometheus metrics for batching, quantization, SIMD

---

## Competitive Analysis

### **FAISS**

* **GPU**: The industry benchmark for NVIDIA GPU acceleration. Supports massive parallelization and multi-GPU indexing via **IVF-PQ** and **HNSW-Flat**.
* **SIMD**: Highly optimized C++ core utilizing AVX2, AVX-512, and ARM Neon for maximum throughput on dense vector operations.

### **Longbow**

* **Arrow-Native**: No serialization overhead when interacting with Arrow-based data pipelines or DuckDB.
* **GraphRAG**: Dual-path architecture combining:
  * **Spreading Activation**: Vector-based re-ranking using HNSW Layer 0 graph expansion
  * **Knowledge Graph**: Triple-based (SPOW) explicit relationships with PageRank & Community Detection
* **Hybrid Search**: Seamless integration of vector similarity with full-text search (BM25) and metadata filtering.
* **TurboQuant**: Novel two-stage compression achieving extreme density with fast search.