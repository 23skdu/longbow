# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-22

---

## 🎯 IMMEDIATE REMEDIATION (2026-04-21: 0.1.9-rc4)

These items were identified during a deep code review as being non-functional mocks or stubs that block production reliability.
- [x] **Metal ONNX Acceleration (Real Implementation)**:
  - [x] Replace character-overlap scoring in `internal/onnx/metal/engine_impl.m` with actual Metal Compute Shaders (MSL).
  - [x] Implement `Embed` in `internal/onnx/metal/engine.go`.
- [x] **Learned Index Hardening**:
  - [x] Remove hardcoded `[]string{"default"}` from `getMonitoredCollections`.
  - [x] Implement live metrics collection for latency and recall in `RuntimeIndexAdapter`.
- [x] **Tokenizer Reliability**:
  - [x] Remove dummy vocab fallback in `internal/ml/tokenizer.go`; require `vocab.txt`.
- [x] **GPU Detection Accuracy**:
  - [x] Implement actual Metal memory detection in `internal/gpu/detection.go` instead of 16GB estimate.

---

## ✅ COMPLETED (2026-04-21): Learned Index k-NN Wiring

Fully wired the `IndexPerformancePredictor` adaptive index scorer. Previously the
`AddTrainingSample()` pipeline collected data that was never used. The feedback loop
is now closed.

- [x] **`FeatureNormalizer`**: Online min/max normaliser over 11 `QueryFeatures` dimensions.
- [x] **`extractFeatureVector`**: Dense float64 projection of `QueryFeatures` → normalised vector.
- [x] **`kNNPredict`**: k-nearest-neighbour scorer (inverse-distance-weighted voting, k=7 default).
- [x] **`updateWeights` (LDA)**:  Async goroutine that recomputes `featureWeights` via Fisher
  between-class variance — ensures the k-NN distance metric improves as data accumulates.
- [x] **`AddTrainingSample` rewrite**: Updates normaliser, emits overflow/gauge metrics, triggers
  async weight update when `count >= MinTrainingSamples && interval elapsed`.
- [x] **`Predict` rewrite**: Routes through `kNNPredict` when trained, `getDefaultPrediction`
  when not; emits `longbow_learned_index_predictions_total{method}` counter.
- [x] **`IndexSwitcher` interface + `Rollback` implementation**: Rollback now either calls
  `SwitchIndex` on the wired `IndexSwitcher`, or returns a typed error (no longer a no-op).
- [x] **`LearnedIndexConfig.KNN`**: Configurable k, defaults to 7.
- [x] **Prometheus metrics**: 8 new metrics in `internal/metrics/learned_index_metrics.go`.
- [x] **Unit tests**: 15 new tests covering k-NN correctness, normaliser, weight update
  direction, feedback loop, rollback lifecycle — all passing with `-race`.
- [x] **Fuzz tests**: `FuzzKNNPredict` and `FuzzFeatureNormalizer` in
  `learned_index_knn_fuzz_test.go`.
- [x] **`scripts/unified_benchmark.py`**: New `learned_index` mode with 4-stage validation.
- [x] **Docs**: Updated `vectorsearch.md`, `features.md`, `nextsteps.md`.

---

## ✅ COMPLETED (2026-04-21): EmbeddingGenerator → Learned Index Integration

Connect Longbow's `EmbeddingGenerator` backends (OpenAI, Cohere, HuggingFace, ONNX, WASM) to
the `IndexPerformancePredictor` so that which embedding model is active becomes a discriminating
feature for k-NN index selection. Hybrid (dense+BM25) queries use different optimal index
configurations depending on embedding provider characteristics (latency, dimension, batch behaviour).

- [x] **Extend `QueryFeatures`**: Added `EmbeddingProvider string` and `EmbeddingModel string`.
- [x] **Expand feature vector** (`numFeatures` 11→13): `embedding_provider` (ordinal 0-6) and
  `embedding_model_dim` (ratio relative to 384d reference) added to `featureKeys`,
  `extractFeatureVector`, `initializeWeights`, and `embeddingProviderOrdinal`/`embeddingModelDimRatio`.
- [x] **`SetActiveEmbedding` / `GetActiveEmbedding`**: New mutex-guarded methods on `VectorStore`
  so any layer that provisions an `EmbeddingGenerator` can register context once.
- [x] **`RecordQueryPerformance` signature updated**: Now accepts `provider, model string` and
  populates `QueryFeatures.EmbeddingProvider`/`EmbeddingModel` before `AddTrainingSample`.
- [x] **`SearchHybrid` wired**: Calls `RecordQueryPerformance` after every hybrid search, fetching
  the active embedding context via `GetActiveEmbedding()`.
- [x] **`QueryFeatures.String()` updated** (Ollama path) to include provider/model.
- [x] **Unit tests** (`learned_index_embedding_test.go`): 6 tests covering ordinal stability,
  model dim ratio for all known combos, feature vector length invariant, and end-to-end
  provider-discriminating convergence — all pass with `-race`.
- [x] **`docs/agentmemory.md`**: Agent memory architecture doc.

---

## ✅ COMPLETED (2026-04-21): Learned Index Production Hardening

Finalized production hardening of the adaptive learned index and GraphRAG systems, enabling fully data-driven index management.

- [x] **Transformer Mean Pooling**:
  - Proper pooling across transformer hidden states with attention mask support in `internal/onnx/onnx.go`.
  - Added L2 normalization and performance metrics.
- [x] **Dynamic Sharding**:
  - Auto-rebalancing shards based on node load (thresholds: 0.8 to reduce, 0.4 to increase).
  - Updated `RingManager` and `ConsistentHash` to support runtime vnode adjustments.
- [x] **Unified k-NN Predictor**:
  - Unified `Predict` and `PredictWithEmbedding` search paths in `internal/store/learned_index.go`.
  - Made k-NN the primary decision engine, removing all hardcoded rule-based heuristics.
- [x] **Implementation of `IndexSwitcher`**:
  - Implemented background rebuild and atomic index swaps in `VectorStore`.
  - Added `activeSwitches` tracking to prevent concurrent conflicting migrations.
- [x] **Production Wiring**:
  - Wired `RuntimeIndexAdapter` into `VectorStore` lifecycle.
  - Implemented `MetricsCollector` interface on `VectorStore` to provide latency, recall, and QPS signals to the adapter.
- [x] **Closed-Loop Feedback**:
  - Recorded adaptation outcomes as training samples for the predictor.
  - Implemented "failure decomposition": recorded failed adaptations as negative signals (high latency penalty) to prevent oscillation.

---

## 🎯 NEXT STEPS (2026-04-21: 0.1.9-rc5)

## 🎯 IMMEDIATE PRODUCTION HARDENING (Priority: P0 - Release 0.1.9)

### 🚀 High-Performance Compute & Memory (P0)

- [ ] **Specialized FP16 CUDA Kernels**:
  - [ ] **Task**: Implement specialized FP16 CUDA kernels in `internal/gpu/cuda/kernels.cu` to maximize Tensor Core utilization on RTX 40/50 series.
  - [ ] **Metrics**: Track `longbow_cuda_tensor_core_utilization_ratio` and `longbow_gpu_instruction_throughput`.
  - [ ] **Tests**: Numerical parity tests between FP32/FP16 kernels; benchmark throughput gain on `ancalagon`.
- [ ] **Arrow-Native Metadata Transition**:
  - [ ] **Task**: Transition metadata storage from `map[string]interface{}` to **Arrow-native binary format** (columnar) to eliminate heap fragmentation.
  - [ ] **Metrics**: Track `longbow_metadata_heap_alloc_bytes` and GC pause duration during large-scale (50k+) searches.
  - [ ] **Tests**: Benchmark memory pressure and lookup latency for columnar metadata vs. Go maps.
- [ ] **NUMA-Local Memory Pinning (HNSW)**:
  - [ ] **Task**: Implement NUMA-aware memory allocation and pinning for HNSW layers to reduce cross-socket latency on multi-socket servers (e.g., `ancalagon`).
  - [ ] **Metrics**: Track `longbow_hnsw_cross_numa_latency_ns` and socket-specific cache miss rates.
  - [ ] **Tests**: Comparative latency testing on `ancalagon` with/without NUMA pinning.
- [ ] **AVX-512 Kernels for AMD64**:
  - [ ] **Task**: Implement AVX-512 SIMD kernels for distance calculations (`L2`, `Cosine`, `Dot`) in `internal/simd/avx512.go`.
  - [ ] **Metrics**: Track `longbow_cpu_instruction_set_active{isa="avx512"}` and cycle-per-vector efficiency.
  - [ ] **Tests**: Verify numerical parity against generic and AVX2 baselines; test on `ancalagon` Intel i7.

---

## 🎯 REMAINING WORK

### Stability & Production Readiness (Priority: CRITICAL)

- [x] **GraphRAG Indexing Stability**:
  - [x] Resolved reentrant `growMu` deadlock in `InsertWithVector` during large-scale (25k+) ingestion.
  - [x] Hardened `searchLayer` against nil context dereferences in background workers.
  - [x] Eliminated redundant atomic `nodeCount` updates to reduce cache contention.
- [x] **Release 0.1.9 Performance Benchmarking**: Finalized the full performance matrix across 14 dtypes and 8 batch sizes (128d to 768d).
- [ ] **Performance Optimizations (Post-0.1.9)**:
  - [ ] **Dynamic Neighbor Selection**: Implement heuristic-based neighbor pruning during `SearchHybrid` to reduce graph traversal overhead when `alpha < 0.5`.
  - [ ] **Lock-Free Search Context**: Transition `ArrowSearchContext` from a pool to a lock-free thread-local storage pattern for lower hot-path latency.
  - [ ] **Asynchronous Graph Enrichment**: Move `AddEdge` operations to a dedicated background worker to decouple logical graph updates from physical vector ingestion.

## 🎯 CURRENT RELEASE: 0.1.9-rc3

- **Total Coverage**: ~53% (Target: >95%)
- **Status Headroom**: Functional features complete. Focus shifted to deep unit test coverage.

### Completed (Session 0.1.9-rc3-B)

- [x] Parquet High-Throughput IO (+tests)
- [x] ML Cross-Encoder Integration (+mock tests)
- [x] Wazero WASM Runner Finalization
- [x] Adaptive Index Lifecycle Testing
- [x] Storage Backend Unit Tests (FileBackend)
- [x] Query Engine Extended Type Coverage (Int32, Uint64, Float64, String)
- [x] Sharding Result Aggregator Coverage (Merge & Sort)

- [x] **Apache Arrow Zero-Copy & Performance Remediation**:
  - [x] **`ExtractVectorFromArrow` Optimization**:
    - [x] Remove `copy()` in `float32` path; return direct slice from Arrow memory.
    - [x] Implement type-specific zero-copy extraction pointers (`ExtractVectorF32`, `ExtractVectorInt8`, etc.).
    - [x] Documented Pattern: Use `ExtractVector<Type>` for O(1) raw memory access; avoid `ExtractVectorFromArrow` in compute-heavy loops.
  - [x] **`ExtractIDs` Hardening**:
    - [x] Use `int64` keys for numeric ID columns to avoid `strconv.FormatInt` and string allocations.
    - [x] Pool `idMap` allocations to reduce GC pressure during high-throughput ingestion.
  - [x] **Branchless SIMD Logic**:
    - [x] Replace `if/else` branches in `matchInt64Generic` and other element-wise comparison loops with bitwise branchless logic.
  - [x] **Bulk Ingestion Throughput**:
    - [x] Optimize `applyBatchToMemory` to avoid row-by-row vector extraction for `DiskStore` (implemented `BatchAppendArrow`).
    - [x] Transition `IngestBatch` from `RecordBuilder` (row-at-a-time) to Arrow batch column-wise construction.

- [x] **Performance Benchmarking & Orchestration**:
    - [x] Hardened `scripts/unified_benchmark.py` for sequential, multi-port execution.
    - [x] Fixed race conditions in `ZeroAllocTicketParser` and data type inference bugs for `int8`/`uint8`.
    - [x] Optimized server logging to reduce I/O overhead during search benchmarks.
- [x] **Final 0.1.9 Tagging**: Completed full performance matrix validation and release tagging (0.1.9-rc5).

## 🚀 Future Roadmap (0.1.10+)

- [ ] **Google TPU v7x (Ironwood) Support**:
  - **Phase 1: Architecture & Detection**:
    - Implement `TPUDetector` in `internal/gpu/detection.go` for `linux/amd64`.
    - Detect dual-chiplet topology and NUMA affinity (2 NUMA nodes per VM).
  - **Phase 2: XLA-Backed Inference**:
    - Implement `TPUBackend` using OpenXLA/Pallas for custom vector kernels.
    - Optimize for 192GB HBM per chip and multi-tier memory (VMEM/HBM/PCIe).
  - **Phase 3: Observability & Metrics**:
    - Track `longbow_tpu_hbm_usage_bytes`.
    - Track `longbow_tpu_core_utilization_ratio` (TensorCore vs SparseCore).
    - Monitor D2D (Die-to-Die) interconnect latency.
  - **Phase 4: Verification & Stability**:
    - **Unit Tests**: Mock TPU devices using `libtpu` stubs; verify host-to-device tensor transfers.
    - **Fuzz Tests**: `FuzzTPUKernels` to verify boundary conditions in XLA-compiled MSL-equivalent kernels.
    - **Benchmark**: Comparative parity testing between TPU v7x, CUDA 12.6, and Metal.
- [ ] **Cross-Shard Atomic Commits**: Two-phase commit protocol for cross-shard vector updates.
- [ ] **KV-Integrated Indexing**: Native integration with FoundationDB for metadata-heavy searches.

---

## ✅ VERIFIED COMPLETED (2026)

- [x] **Post-0.1.9 Remediation Plan**: Completed full implementation of functional ML/IO/Infra layers (WordPiece, Wazero, Parquet, Darwin NUMA).
- [x] **Parallel SQ8 Ingestion Stabilization (0.1.9)**: Resolved structural races, deadlocks, and recall failures (1/100) in the HNSW bulk ingestion engine.
- [X] **Zero-Copy Tensor Stream**: Direct GPU-to-GPU tensor transfer via Arrow Flight (RoCEv2/PeerDirect).
- [x] **Zero-Copy HNSW Ingest**: Direct Arrow-to-HNSW memory mapping for zero-copy bulk ingestion.
- [x] **ONNX Multi-Backend Benchmarks**: Comprehensive benchmarking suite covering CPU, CUDA, and Metal backends.
- [x] **Store Modularization (Phases 1-6)**: Cleanly decoupled HNSW internals into modular sub-packages.
- [x] **Lock-Free Adjacency (Layer 0)**: Optimized high-contention graph updates for 100k+ TPS ingestion.
- [x] **Numerical Parity & FP64 Match**: Verified SIMD kernels against high-precision float64 baselines.
- [x] **Adaptive M-Param & Search Context**: Dynamic connectivity scaling and pooled context management.
- [x] **Advanced SQL (Subqueries/CTE)**: Nested query resolution and CTE support fully integrated.
- [x] **Metal ONNX & CUDA Backend**: Functional GPU acceleration on macOS ARM64 and Linux NVIDIA.
- [x] **Core Coverage Coverage**: Stabilized ~67% statement coverage across core performance packages.

---

## Architecture Notes

### Build Tags - Expected Stubs (NOT Issues)

- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`
