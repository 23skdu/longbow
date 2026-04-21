# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-21

---

---

## 🎯 IMMEDIATE REMEDIATION (2026-04-21: 0.1.9-rc4)
 
These items were identified during a deep code review as being non-functional mocks or stubs that block production reliability.
 
- [ ] **Metal ONNX Acceleration (Real Implementation)**:
    - [ ] Replace character-overlap scoring in `internal/onnx/metal/engine_impl.m` with actual Metal Compute Shaders (MSL).
    - [ ] Implement `Embed` in `internal/onnx/metal/engine.go`.
- [ ] **Learned Index Hardening**:
    - [ ] Remove hardcoded `[]string{"default"}` from `getMonitoredCollections`.
    - [ ] Implement live metrics collection for latency and recall in `RuntimeIndexAdapter`.
- [ ] **Tokenizer Reliability**:
    - [ ] Remove dummy vocab fallback in `internal/ml/tokenizer.go`; require `vocab.txt`.
- [ ] **GPU Detection Accuracy**:
    - [ ] Implement actual Metal memory detection in `internal/gpu/detection.go` instead of 16GB estimate.
 
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

## 🎯 IN PROGRESS (2026-04-21): Learned Index Production Hardening

Address critical architectural gaps in the adaptive learned index system to move from
"aspirational" to "production-grade".

### Plan

- [ ] **Unified k-NN Predictor**:
    - Unify `Predict` and `PredictWithEmbedding` search paths.
    - Make k-NN the primary decision engine for all query types.
    - Remove hand-coded heuristics and complexity biases (move to data-driven features).
- [ ] **Implementation of `IndexSwitcher`**:
    - Implement the `IndexSwitcher` interface on `VectorStore`.
    - Create `SwitchIndex(collection, indexType)` which triggers a background rebuild
      of the index from source records followed by an atomic swap.
- [ ] **Production Wiring**:
    - Wire `RuntimeIndexAdapter` into the `VectorStore` lifecycle.
    - Establish a bidirectional link: Store calls Predictor; Adapter (watching Store) calls Switcher.
- [ ] **Closed-Loop Feedback**:
    - Record adaptation outcomes (success, failure, rollback) as training samples.
    - Implement "failure decomposition": record degradation events as negative learning
      signals to prevent repetitive bad advice.

---

## 🎯 REMAINING WORK

### Stability & Production Readiness (Priority: CRITICAL)

- [x] **Release 0.1.9 Deployment**: Finalize the multi-platform Docker push (ARM64 Metal / AMD64 NVIDIA) and tag the 0.1.9 production release.
- [x] **Gosec Hardening**: Systematically address the remaining 14 high-confidence security findings in the `internal/simd` and `internal/gpu` CGO bridge layers.
- [x] **Expand Test Coverage**: Expand unit and integration test suites across `internal/store/core`, `internal/onnx`, and `internal/simd`. Added comprehensive SIMD test suite and core search context lifecycle tests. Achieved 100% coverage in `internal/onnx`.


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

### Immediate Next Steps (Coverage Push)

1. **Security & RBAC Coverage**: Implement tests for `internal/store/rbac.go`.
2. **GPU Mocking**: Create a mock layer for `internal/gpu`.
3. **Remote Storage Mocks**: Mock S3/GCS in `internal/storage`.
4. **Final 0.1.9 Tagging**: Once coverage hits critical mass (>80%+).

## 🚀 Future Roadmap (0.1.10+)

- [ ] **Transformer Mean Pooling**: Proper pooling across transformer hidden states.
- [ ] **Dynamic Sharding**: Auto-rebalancing shards based on node load.

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
