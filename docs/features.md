# Longbow Features (2026)

Longbow is designed to provide FAISS-level performance with Arrow-native ergonomics and GraphRAG integration.

**Last Updated**: 2026-11-13

---

## Implemented Features

### Performance & Scalability

- **TurboQuant (TQ) V2 & TurboQuant2**: Polar quantization with a configurable angular bit depth of 1-8 bits, plus dedicated **TurboQuant2** (2-bit quantization with random rotation and QJL residual correction for retrievable ANN search at 16x compression with ~0.75+ cosine similarity).
- **AVX-512 FP16 & ARM SVE2**: Native half-precision AVX-512 FP16 instructions (`_Float16`) and ARM SVE2 dynamic vector length kernels for next-generation x86_64 and ARMv9 architectures.
- **Intel AMX Matrix Tile Dispatch**: Hardware matrix tile acceleration (`tileloadd`, `tdpbf16ps`) on Sapphire Rapids and Emerald Rapids Xeon CPUs.
- **PQ ADC Fast-Path**: Asymmetric Distance Computation with SIMD 4-bit lookups (`vpshufb`/`tbl`) for accelerated Product Quantization search.
- **2MB Hugepages Allocator**: Native `MAP_HUGETLB` / `MADV_HUGEPAGE` memory allocator reducing TLB misses during graph traversals.
- **Flat Adjacency Layout & Vectorized Bitmask**: Contiguous memory neighbor lists for L1/L2 cache locality and 64-bit SIMD visited bitmask.
- **Multi-Centroid HNSW Entry Points**: Multi-centroid clustering tracking top-layer centroids across disconnected graph partitions.
- **Dynamic SQ8 Percentile Calibration**: Automated outlier rejection via percentile dynamic range fitting `[p_low, p_high]`.
- **Autonomous efSearch Tuning**: Integrated PID-controller for dynamic search depth adjustment, replacing static heuristics with real-time feedback-driven recall targeting.
- **NEON SIMD Parity**: Full metadata filtering acceleration for ARM64 (macOS/Apple Silicon) via hand-optimized NEON assembly kernels.
- **Lock-Free Ingestion Workers**: High-performance ingestion pipeline using `LockFreeRingBuffer` and adaptive batching to eliminate mutex contention.
- **Runtime Learned Index**: `IndexPerformancePredictor` selects the optimal ANN index type per query using a k-NN classifier.
- **Adaptive Flat to HNSW Migration**: Automated, zero-downtime promotion from flat scan to HNSW indexing.
- **GPU Complex Type Kernels**: Native CUDA kernels for complex128/complex64 L2 distance, dot product, and cosine similarity, eliminating CPU fallback for complex vector search.
- **Conditional Math Dispatch**: `LONGBOW_MATH_DISPATCH` env var for selective EMLGo routing by type and scale — emlgo for complex types + TQ above 50k, standard for int/float below 50k.
- **Benchmark Multi-Run**: Statistical aggregation with mean/stdev reporting across multiple benchmark runs, plus memory soak testing for long-duration stability validation.

### Advanced Quantization Suite

- **Product Quantization (PQ)**: Sophisticated sub-vector quantization with optimized codebook training and SIMD ADC fast-path for extreme memory reduction (16x-32x) while maintaining high recall.
- **Scalar Quantization (SQ8)**: Integrated support for 8-bit scalar quantization with dynamic percentile calibration directly within the HNSW metadata layer for 4x memory reduction.
- **TurboQuant2**: Dedicated 2-bit quantization with random rotation and 1-bit residual correction.
- **Binary Quantization (BQ)**: Extremely fast Hamming-distance based search for binary-encoded vectors (32x memory reduction).
- **Float16 Support**: Native half-precision vector storage and distance computation for 2x memory savings.

### Specialized Search Capabilities

- **Full Multi-Modal & Unified Search Suite**: Supports Dense ANN/k-NN, Vector Search by ID (`search_by_id`), Filtered search (`filter_expr`, subqueries, CTEs), Sparse BM25 keyword search (`alpha=0.0`), Advanced Full-Text search (Boolean AST, phrases, fuzzy `~N`, snippets, facets), Hybrid RRF search (`raw_hybrid`), Distance-Threshold Range Search (`RangeSearch`), Federated Cross-Collection Search (`FederatedSearch`), Multi-Modal cross-modal search (`MultiModalSearch`), and Hybrid Vector-Graph Recommendations (`Recommend`).
- **Distributed GraphRAG**: Support for cross-node BFS and activation propagation protocols (`graph_alpha`/`graph_depth`, `traverse-graph`, `GraphRAGExpand`, `GlobalGraphRAG`), enabling traversal of multi-billion node knowledge graphs across clusters.
- **Temporal Versioning**: Point-in-time `as_of`, time `range`, count `sliding_window`, duration `sliding_window_time`, version history, and temporal aggregations.
- **Persistent HNSW Memory Mapping**: Direct Arrow-backed `mmap` for HNSW graph storage, eliminating indexing load times and enabling instant-on cold starts.
- **Geo-Search Engine**: Native support for Haversine `radius`, bounding `box`, and geo+vector `hybrid` indexing using AVX-accelerated Quadtrees, with an opt-in Morton/Z-order grid for write-heavy workloads.
- **SQL Analytical Functions**: Full support for `ROW_NUMBER`, `RANK`, and windowing functions.
- **Multi-Type Filter Evaluator**: Native SIMD-accelerated support for Int32, Uint64, Float64, Boolean, and String comparisons.
- **Automatic Sharding**: Distributed index management splitting large datasets into shards based on growth.

### Unified ML Inference Engine

- **Cross-Platform WASM Runner (Wazero)**: Full integration of the `wazero` runtime for cross-platform execution of Transformer-based models (embeddings and rerankers) without local library dependencies.
- **Production-Grade Reranking**: Implementation of the `Cross-Encoder` strategy using subword WordPiece tokenization and normalized Transformer scoring.
- **Memory-Efficient Tokenization**: Zero-copy token storage and pooled transformer context management for high-concurrency inference.

### Security & Reliability

- **Hardened CGO Bridge**: Remediated all high-confidence security findings in SIMD/GPU layers and removed vulnerable FAISS dependencies.
- **Audited Subprocess Isolation**: Subprocess execution for GPU discovery is now fully audited and hardened.
- **Comprehensive Test Coverage**: Every package in the repository has active test coverage, which CI enforces (`.github/workflows/ci.yml` runs `go test -v -short -coverprofile=coverage.txt ./...`, then fails the build if `go test -list '.*'` reports any `[no test files]` or if `scripts/check_fuzz_coverage.sh` finds a package without a `Fuzz*` target). The gate is per-package presence (unit tests and fuzz targets), not a numeric line-coverage threshold, so no ">95%" figure is claimed here. Race-condition validation runs separately under `-race` via `make race`.

### Portability & Infrastructure

- **Darwin Core Awareness**: Mach-level processor cluster identification for Apple Silicon (macOS) for core-type-aware worker affinity.
- **Formalized Maintenance Scheduler**: Automated background repair, tombstone reclamation, and memory-limit enforcement tasks.
- **Zero-Copy Network-to-GPU**: libibverbs CGO bindings for Linux/RoCEv2 and RDMA-aware Arrow Flight handshake.
- **Python SDK (Zero-Copy)**: High-performance client using Arrow Flight for wire-speed ingestion and retrieval. Features native NumPy/Pandas integration and automatic vector type inference to eliminate serialization overhead.
- **Administrative CLI**: Comprehensive Go-based tool for cluster management, geospatial search, and large-scale S3 Parquet ingestion. Enables terminal-based GraphRAG operations and dataset lifecycle management.

### Monitoring & Observability

- **Full Prometheus Instrumentation**: Hardware-level metrics including GPU utilization, memory bandwidth, and WASM runtime latency profiles.
- **Distributed Tracing**: End-to-end tracing for the ONNX inference pipeline across distributed sharding boundaries.

### Enterprise Cloud, Security, APIs & Query Intelligence (Items 1–15)

- **Managed Cloud Control Plane & Multi-Region Replication (`internal/store/cloud_control_plane.go`)**: Provision serverless/dedicated/enterprise clusters with tier SLA policies (availability, RPO/lag, RTO, P99 latency), cross-region SHA-256 checksummed replication (`async`/`semi_sync`/`sync`), lowest-lag read routing, failover promotion, and live SLA evaluation.
- **First-Class REST/HTTP v1 & GraphQL APIs (`internal/store/rest_graphql.go`)**: Full REST CRUD, dense/filtered, by-id (`/search/by-id`), hybrid (`/hybrid`), advanced full-text (`/text-search`), geo (`/geo-search`), temporal (`/temporal-search`), multi-modal (`/multimodal-search`), range (`/range-search`), federated (`/api/v1/search/federated`), recommend (`/recommend`), columnar aggregations (`/aggregations`), OpenAPI v3.0.3 schema (`/api/v1/openapi.json`), and pure-Go GraphQL engine (`/api/v1/graphql` + `GraphQLQuery` `DoAction`) with variables, aliases, mutations, and introspection.
- **Multi-Tenant RBAC, API Keys & RLS (`internal/store/rbac.go`, `internal/security/`)**: Roles (`admin`, `tenant-admin`, `writer`, `reader`), key rotation with grace windows and immediate revocation, namespace/dataset isolation, row-level metadata predicates (`SetRLSPolicy`), HTTP Bearer/`X-API-Key` middleware, and immutable audit logging ([security-compliance.md](security-compliance.md)).
- **Encryption at Rest, TLS/mTLS & KMS (`internal/storage/encryption.go`, `internal/security/kms.go`)**: AES-256-GCM length-framed encryption for disk spill and WAL, TLS 1.2/1.3 mutual authentication, SIGHUP/interval certificate reloading, and Local/Vault Transit envelope key management.
- **Online Backup & Point-in-Time Recovery (`internal/store/backup.go`, `backup_pitr.go`)**: Online dataset snapshot archives with SHA-256 checksum verification, local/S3/GCS destinations, retention pruning, and WAL replay PITR.
- **Real-Time CDC & Streaming Ingestion (`internal/store/streaming_connectors.go`)**: Kafka and Pulsar connectors with Confluent 5-byte wire header / HTTP Schema Registry validation, `SequenceDeduplicator` exactly-once watermarks, backpressure pause/resume, and dead-letter queue capture.
- **Multi-Modal Pipelines (`internal/store/multimodal.go`)**: PNG/JPEG spatial image and PCM16 WAV audio encoders, HTTP/MLBackend embedding adapters, Arrow `blob+modality+vector` storage, Parquet/S3 media ingestion, and cross-modal search.
- **Advanced Reranking & Query Rewriting (`internal/store/reranker_advanced.go`)**: Cross-encoder, OpenAI/Ollama LLM, and hybrid rerankers with LRU/TTL caching, synonym/multi-query/HyDE query rewriting, and NDCG@K/MRR evaluation harness.
- **Serverless/Edge SDKs & WASM (`pkg/wasmclient`, `cmd/longbow-wasm`, `sdk/edge-js`)**: Pure-Go WASM local HNSW index with offline-first LWW/VectorClock sync, TypeScript edge SDK, and HTTP `/api/v1/search` & `/api/v1/edge/sync` endpoints ([edge-wasm.md](edge-wasm.md)).
- **Cost/Usage Metering & Quotas (`internal/store/metering.go`)**: Per-tenant query/vector/storage/egress/rerank metering, soft warnings and hard quota rejections, JSON/CSV/Webhook/OTel billing exports, and `/api/v1/usage` endpoints.
- **Elastic Sharding & Auto-Scaling (`internal/store/elastic_sharding.go`)**: Shard split/merge and rebalancing controller, load-based scale-up/scale-down recommendations, cluster peer membership and failover, plus Helm HPA integration.
- **Schema Validation, Constraints & Evolution (`internal/store/schema_constraints.go`)**: Dataset schema registry with `NOT NULL`, `UNIQUE`, `CHECK`, and typed metadata enforcement during ingestion, backward/forward/full compatibility verification, and record batch migration.
- **Materialized Views & Aggregations (`internal/store/materialized_views.go`)**: Real-time streaming and scheduled `GROUP BY` aggregations (`count`, `sum`, `avg`, `min`, `max`) with query planner lookup (`TryPlanQuery`).
- **Advanced Full-Text Search & Analytics (`internal/store/text_search_advanced.go`)**: Boolean AST (`AND`/`OR`/`NOT`), phrase matching, Damerau-Levenshtein fuzzy terms (`~N`), `<mark>` snippet highlighting, categorical faceting, per-dataset BM25 (`k1`, `b`) tuning, and zero-result analytics.
- **Explainability & Vector Provenance (`internal/store/provenance_explain.go`)**: Dense/sparse/RRF/graph score breakdowns, HNSW traversal hop trace debugging (`ExplainSearch` / `DebugSearchPath`), model/source/chunk provenance storage, and Python SDK table formatting.

---

## Competitive Landscape

### Feature Matrix

| Feature | Chroma | Milvus | Qdrant | **FAISS** | Pinecone | TencentDB | **Longbow** |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Focus** | Prototyping | Massive Scale | Speed & Efficiency | **Perf Library** | Managed SaaS | Enterprise SaaS | **Structural Discovery** |
| **Quantization** | No | **RaBitQ / PQ** | PQ/SQ/BQ | **PQ/SQ/OPQ** | Proprietary | PQ / SQ | **IVF-OPQ / TurboQuant (4/8-bit)** |
| **Architecture** | SQLite | Distributed | Rust | **C++/CUDA** | Closed/Cloud | Distributed (OLAMA) | **Zero-Copy Arrow** |
| **GPU Support** | CPU-Only | **Tier 1** | **Tier 1** | **Tier 1 (NVIDIA)** | Managed | Managed | **Tier 1 (Metal/CUDA)** |
| **SIMD Optim.** | Library | Extensive | **Native** | **Extensive** | Managed | Extensive | **Custom AVX2/AVX512/NEON** |
| **GraphRAG** | No | Basic | No | No | No | External (Langchain) | **Dual-Path: Spreading + Knowledge Graph** |
| **Temporal** | No | No | No | No | No | No | **Native Versioning (Opt-In)** |
| **Geo-Spatial** | No | No | Native | No | No | Basic | **Native Quadtree (default) + opt-in Morton Grid** |

#### Geo-Spatial Index Selection

Two indexes implement the same `GeoPointIndex` contract and return the same point set for the same inputs:

| `IndexType` | Structure | Writes | Reads | Default |
| :--- | :--- | :--- | :--- | :--- |
| `quadtree` | Recursive subdivision, adaptive buckets | Allocates a child node per split | Fastest | **Yes** |
| `morton` | Uniform grid of 64-bit Z-order (Morton) coded cells, points in one contiguous arena | No per-insert node allocation; ~2.3x faster | 17% (selective box) to 40% (global box) slower | No |

The Morton grid (`internal/store/morton_grid.go`) interleaves every indexed point into a single contiguous arena indexed by the Morton code of its cell, so an insert only grows flat slices instead of calling `Quadtree.subdivide`. Its cell resolution is fixed (12 bits per axis by default, i.e. ~5 km x 8 km cells), which is why queries pay for the non-adaptivity and `quadtree` remains the default. Select it where the write path dominates, and tune the cell size with `NewMortonGridWithResolution`.

### Performance Parity (0.1.9)

Internal benchmarks on 1M vectors (1536D) show that Longbow is within 5% of FAISS's raw C++ throughput while providing much better memory efficiency via Arrow zero-copy memory management.

| Metric | FAISS (IVF-PQ) | Longbow (IVF-HNSW) | Longbow (TurboQuant) |
| :--- | :--- | :--- | :--- |
| Search Latency (1M) | ~2.5ms | ~1.8ms | ~0.4ms |
| Memory Overhead | 2.5x | 1.8x | 0.25x |
| Build Throughput | 10k/s | 25k/s | 40k/s |
| Compression Ratio | 4x | 1x | 2.6x-4.7x (4-8 bit, 1536D) |
| SIMD Optimization | Partial (AVX2) | Full (AVX-512; AMX on amd64 only) | Full |
| TPU Acceleration | No | Yes | No (Use CPU TQ) |
| Zero-Copy Flight | No | Yes | Yes |

**TurboQuant (0.1.9 New)**: Two-stage vector compression combining Polar Quantization with QJL correction, with a configurable angular bit depth. See [TurboQuant Compression](#turboquant-compression) for what the encoder actually buys.

#### TurboQuant Compression

The encoded size is fixed by `PackedSize(dims, bits)` in `internal/store/index/turboquant.go`:

```text
PackedSize(dims, bits) = align32(4 + ceil((p2-1)*bits/8) + ceil(p2/8))
p2 = smallest power of two >= dims
```

The 4 leading bytes are the radius, the next block is the `(p2-1)` packed angles, the final block is the `p2` QJL sign bits, and the result is rounded up to a 32-byte boundary for GPU warp alignment. Dividing `dims*4` by that gives the real compression:

| Dims | 2-bit | 4-bit | 8-bit |
| :--- | :--- | :--- | :--- |
| 128 | 8.0x | 5.3x | 3.2x |
| 384 | 6.9x | 4.4x | 2.5x |
| 768 | 7.4x | 4.6x | 2.6x |
| 1536 | 7.7x | 4.7x | 2.6x |

So the earlier "up to 64x" figure was never reachable. The layout stores one angle code *and* one QJL sign bit per coordinate regardless of depth, which caps the ratio at roughly 8-11x even at 1 bit — and at 1-3 bits the round-trip cosine collapses (see the `TurboQuantParams.BitsPerAngle` comment). **Plan on ~2.5x-4.7x for the 4-8 bit depths that actually retrieve.**

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
- **TurboQuant**: 4-bit and 8-bit compression modes (the encoder accepts 1-8 bits, but 4-8 is the range that round-trips above cosine 0.90)
- **Auto-Tuning**: Automatic selection between float32/int8/PQ/TQ based on memory/recall

#### Performance

- **SIMD**: Complete AVX2, AVX512, and NEON kernels
- **Batching**: DoPut bulk path for >=100 vectors
- **io_uring**: Linux async I/O for WAL operations
- **GPU Complex Kernels**: Native CUDA L2/dot/cosine for complex128/complex64
- **Conditional Dispatch**: EMLGo routing by type and scale via `LONGBOW_MATH_DISPATCH`
- **Benchmark Multi-Run**: Statistical mean/stdev across runs + memory soak tests

#### Search

- **Temporal**: Native versioning (opt-in, `LONGBOW_TEMPORAL_ENABLED=true`; the index is only constructed when the flag is set)
- **Hybrid**: Vector + BM25 + metadata filtering
- **GraphRAG**: Dual-path (Spreading + Knowledge Graph) with PageRank & Community Detection
- **Complex Vector Search**: First-class complex128/complex64 support across CPU and GPU

#### Quality

- **Fuzz Tests**: IVF index build, TurboQuant encode/decode
- **Metrics**: Prometheus metrics for batching, quantization, SIMD
- **Memory Soak**: Long-duration stability validation for production readiness
