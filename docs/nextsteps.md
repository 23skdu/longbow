# Longbow Next Steps & Recommendations

**Last Updated**: 2026-03-28
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)

---

## 🔴 TOP PRIORITY — API & Server Quality Improvements

The following seven items represent the highest-priority engineering work for the next release cycle.
Each item includes a concrete implementation plan, required unit test coverage, and Prometheus metrics to add.

---

### 1. EOF Detection Consistency

**Problem**: The `arrow-go` library returns `io.EOF` (a Go sentinel), but several client-side consumers
check for the string `"EOF"`. This mismatch causes silent stream termination failures across language
boundaries (Python Archer client, benchmark tool, etc.).

**Plan**:
- In the Arrow Flight `DoGet`/`DoPut`/`DoExchange` server handlers, normalize stream-end signals:
  - Replace bare `io.EOF` propagation with a canonical wrapper in `internal/flight/`.
  - Add a helper `IsStreamEOF(err error) bool` that checks both `io.EOF` and `errors.Is(err, io.EOF)`.
- Client SDKs (`longbowclientsdk/`, `clients/`) should call `IsStreamEOF` instead of string-comparing.
- Update protocol documentation in `docs/arrow-protocol.md`.

**Unit Tests** (`internal/flight/eof_test.go`):
```go
// TestIsStreamEOF_GoSentinel — verify io.EOF → true
// TestIsStreamEOF_WrappedEOF — verify fmt.Errorf("context: %w", io.EOF) → true
// TestIsStreamEOF_StringOnlyEOF — verify errors.New("EOF") → true (legacy compat)
// TestIsStreamEOF_OtherError — verify unrelated error → false
// TestDoGetHandler_EOFNormalisation — integration: DoGet returns EOF, client handler
//   sees IsStreamEOF == true and does NOT treat it as an error
```

**Prometheus Metrics** (add to `internal/metrics/search_metrics.go`):
```go
// EOFNormalisationTotal — counter{direction="client|server", protocol="arrow|grpc"}
// StreamTerminationErrors — counter{direction, error_type}
```

---

### 2. Expose Search Consistency Levels

**Problem**: The server supports HNSW with variable ef (entry factor), but there is no API-visible way
for clients to request `eventual` (fast, lower ef) vs `strong` (slower, higher ef, more accurate) search.

**Plan**:
- Add `Consistency` field to `SearchOptions` (in `internal/store/vector_types.go`):
  ```go
  type SearchOptions struct {
      IncludeVectors bool
      VectorFormat   VectorDataType
      Filter         any
      FilterExpr     FilterExpr
      ExactK         bool
      Ef             int
      // NEW
      Consistency string // "eventual" | "strong" | "" (default = eventual)
  }
  ```
- In `VectorSearchRequest` proto/Arrow exchange, surface as `consistency` field.
- In `internal/store/vector_search_exchange.go`, map `consistency == "strong"` → `ExactK = true`
  and auto-promote `Ef` to `max(Ef, 2*K)`.
- Document in `docs/vectorsearch.md` with benchmark tradeoff table.

**Unit Tests** (`internal/store/consistency_test.go`):
```go
// TestConsistencyLevelEventual — search with consistency="eventual", assert Ef unchanged
// TestConsistencyLevelStrong — search with consistency="strong", assert ExactK=true & Ef >= 2*K
// TestConsistencyLevelDefault — empty string → same as "eventual"
// TestConsistencyLevelInvalid — unknown value → wrapped error with exact message
// TestVectorSearchExchange_ConsistencyMapping — end-to-end: request consistency flows through
//   VectorSearchExchange into SearchOptions correctly
```

**Prometheus Metrics** (add to `internal/metrics/search_metrics.go`):
```go
// SearchConsistencyLevelTotal — counter{dataset, level="eventual|strong"}
// SearchStrongModeLatencySeconds — histogram{dataset} (latency overhead of strong mode)
```

---

### 3. Dynamic Dimension Handling

**Problem**: Clients receive `"dimension mismatch (expected 2, got 4)"` when datasets are created
with incorrect or default dimensions. There is no auto-detection, and the error message lacks
actionable detail.

**Plan**:
- Detect dimension from the **first vector** ingested if the dataset was created with `dimension=0`
  (or a sentinel like `-1`). Lock the dimension after first insert.
- Enrich the existing error path with the dataset name, expected dimension, and received dimension:
  ```
  dataset "embeddings": dimension mismatch — expected 768, received 4 (hint: re-create
  the dataset with CreateDataset(dimension=4) or verify embedding model output dimension)
  ```
- Add `DimensionAutoDetected bool` to dataset metadata (stored in WAL header).
- Gate: once dimension is locked, reject inserts with a clear sentinel `ErrDimensionLocked`.

**Unit Tests** (`internal/store/dimension_test.go`):
```go
// TestAutoDimension_FirstVectorSets — insert first vec of dim 128 into dim-0 dataset → locked
// TestAutoDimension_SecondVectorMatchesDim — second vec dimension matches → ok
// TestAutoDimension_MismatchError — second vec wrong dimension → ErrDimensionLocked with context
// TestDimensionMismatchErrorMessage — assert message contains "expected X, received Y"
// TestDimensionMismatchErrorMessage_WithDatasetName — assert message includes dataset name
// TestCreateDataset_ExplicitDimension — explicit dim= > 0 → auto-detect disabled
```

**Prometheus Metrics** (add to `internal/metrics/storage_metrics.go`):
```go
// DatasetDimensionAutoDetectTotal — counter{dataset, result="success|conflict"}
// DatasetDimensionMismatchTotal — counter{dataset}
```

---

### 4. GraphRAG Server-Side Documentation & Metrics

**Problem**: The `GraphAlpha` and `GraphDepth` parameters in the search API control a spreading-
activation graph traversal for GraphRAG workloads. Neither the algorithm nor the parameters are
documented, and there are no metrics for graph re-ranking performance.

**Plan**:
- Create `docs/graphrag_internals.md` covering:
  - The spreading activation algorithm: seed nodes → BFS/beam expansion with alpha decay.
  - `GraphAlpha` (damping coefficient 0.0–1.0) and `GraphDepth` (max BFS hops) semantics.
  - Worked example showing retrieval quality improvement vs latency tradeoff.
  - Recommended values: alpha=0.85, depth=2 for most RAG workloads.
- Update `docs/graph_rag.md` and `docs/graph_navigation.md` with cross-references.
- Reference existing `internal/metrics/graph_navigation_metrics.go` and extend it.

**Unit Tests** (`internal/store/graphrag_spreading_test.go`):
```go
// TestGraphAlpha_ZeroCollapsesToSingleHop — alpha=0.0 → only direct neighbors returned
// TestGraphAlpha_OneFullSpread — alpha=1.0 → full depth traversal, no damping
// TestGraphDepth_Zero — depth=0 → only seed node returned
// TestGraphDepth_Negative — depth<0 → error
// TestGraphAlpha_OutOfRange — alpha > 1.0 → error with message "alpha must be in [0.0, 1.0]"
// TestSpreadingActivation_ScoreDecay — assert scores decrease monotonically with hop distance
// TestGraphRAG_ReRankingResultOrder — after graph re-rank, higher alpha → more graph-influenced order
```

**Prometheus Metrics** (extend `internal/metrics/graph_navigation_metrics.go`):
```go
// GraphRAGOperationsTotal — counter{dataset, result="success|empty|error"}
// GraphRAGAlphaValue — histogram{dataset} (distribution of alpha values used)
// GraphRAGDepthValue — histogram{dataset} (distribution of depth values used)
// GraphRAGReRankLatencySeconds — histogram{dataset}
// GraphRAGSeedNodesTotal — histogram{dataset} (number of ANN seeds before graph expansion)
// GraphRAGExpandedNodesTotal — histogram{dataset} (nodes returned after expansion)
```

---

### 5. Native TurboQuant Storage

**Problem**: Archer (and other clients) store float32 vectors in Longbow and rely on server-side
quantization implicitly. However, Longbow already supports TurboQuant-encoded vectors natively.
Clients have no explicit way to declare `vector_type = "turboquant"` at the API level, so they
cannot opt into the more storage-efficient and faster TurboQuant index path.

**Plan**:
- Add `VectorType` to `VectorSearchRequest` (Arrow exchange metadata and REST/gRPC stubs):
  ```
  vector_type: "float32" | "turboquant" | "int8" | "binary"
  ```
- Create `docs/turboquant_storage.md` documenting:
  - How to create a TurboQuant-indexed dataset: `CreateDataset(vector_type="turboquant", dimension=768)`
  - Wire format: packed `uint8` buffer with TQ header.
  - Benchmark comparison: storage size, ingestion QPS, search QPS vs float32.
- Update `internal/store/vector_types.go` to add `VectorTypeTurboQuant` constant if not already
  aliased from `types` package (verify against `internal/store/types/`).

**Unit Tests** (`internal/store/turboquant_storage_test.go`):
```go
// TestCreateDataset_VectorTypeTurboQuant — create dataset with vector_type=turboquant → ok
// TestInsert_TurboQuantVector — insert pre-encoded TQ vector → stored without re-encoding
// TestInsert_Float32IntoTurboQuantDataset — insert float32 into TQ dataset → server encodes on-the-fly
// TestSearch_TurboQuantDataset — search returns correct neighbours with TQ index
// TestVectorTypeField_PropagatesToSearchOptions — vector_type flows end-to-end from request
//   to SearchOptions.VectorFormat
// TestVectorType_Invalid — unknown vector_type → error with enum list in message
```

**Prometheus Metrics** (add to `internal/metrics/storage_metrics.go`):
```go
// DatasetVectorTypeTotal — gauge{dataset, vector_type} (tracks type distribution at creation)
// TurboQuantEncodingTotal — counter{dataset, direction="client_provided|server_encoded"}
// TurboQuantEncodingLatencySeconds — histogram{dataset}
// TurboQuantStorageBytesTotal — gauge{dataset} (bytes saved vs float32 baseline)
```

---

### 6. GetNeighbors Support

**Problem**: The `GetNeighbors` action fails on the server (likely returns `unimplemented` or panics).
This is a documented, client-visible action that should either be fully implemented or return a clear,
structured error.

**Plan**:
- **Option A (Implement)**: Wire `GetNeighbors` through the HNSW index to call
  `GetLayerNeighbors(id, layer=0)`. Return the neighbor IDs and distances as an Arrow record batch.
  - Add `GetNeighbors(ctx, id VectorID, k int) ([]SearchResult, error)` to the `Index` interface
    in `internal/store/pluggable_index.go`.
  - Implement for HNSW, DiskANN, and IVFFlat (returning `ErrNotSupported` for IVFFlat).
- **Option B (Reject Clearly)**: Return gRPC `codes.Unimplemented` with message:
  ```
  GetNeighbors is not yet supported on this index type. Use SearchVectors with a stored vector as query.
  ```
- **Recommendation**: Implement Option A for HNSW (complete), Option B with clear message for others.

**Unit Tests** (`internal/store/get_neighbors_test.go`):
```go
// TestGetNeighbors_HNSW_ReturnsTrueNeighbors — insert 100 vecs, get neighbors of vec[0] →
//   result set == HNSW layer-0 neighbor list
// TestGetNeighbors_HNSW_KLimitRespected — k=5 returns at most 5 results
// TestGetNeighbors_UnknownID — id not in index → error ErrVectorNotFound
// TestGetNeighbors_IVFFlat_ReturnsNotSupported — IVFFlat returns wrapped ErrNotSupported
// TestGetNeighbors_DiskANN_ReturnsResults — DiskANN returns approximate neighbors
// TestGetNeighbors_ActionHandler_WiresCorrectly — Arrow exchange action "GetNeighbors"
//   reaches the store handler without panic
```

**Prometheus Metrics** (add to `internal/metrics/search_metrics.go`):
```go
// GetNeighborsTotal — counter{dataset, index_type, result="success|not_found|not_supported|error"}
// GetNeighborsLatencySeconds — histogram{dataset, index_type}
// GetNeighborsResultSize — histogram{dataset} (number of neighbors returned)
```

---

### 7. Metrics for Hybrid Search

**Problem**: The existing `internal/metrics/metrics_hybrid.go` tracks BM25 and vector component
latencies, but lacks observability into **result composition** (dense vs sparse ratio), **RRF fusion
timing** as a separate phase, and **graph re-ranking overhead** when used together with hybrid.

**Plan**:
- Add dense vs sparse result ratio tracking to the RRF merge path.
- Separate the RRF fusion phase from other merge operations (already started in
  `HybridSearchMergeDuration` — keep that and add finer-grained sub-phase metrics).
- Add graph re-ranking timing when hybrid + graph is requested together.
- All new metrics go in `internal/metrics/metrics_hybrid.go` alongside existing hybrid metrics.

**Unit Tests** (`internal/metrics/metrics_hybrid_test.go`):
```go
// TestHybridDenseResultRatio_AllDense — 100% dense results → ratio gauge == 1.0
// TestHybridDenseResultRatio_AllSparse — 100% sparse results → ratio gauge == 0.0
// TestHybridDenseResultRatio_Mixed — 70/30 split → ratio ≈ 0.7
// TestRRFFusionDuration_Observed — inject timer, assert histogram bucket incremented
// TestGraphReRankDuration_NonZeroWhenEnabled — with graph depth>0, assert
//   HybridGraphReRankLatencySeconds > 0
// TestGraphReRankDuration_ZeroWhenDisabled — depth=0 → histogram not recorded
// TestHybridResultComposition_Label — counter labels match: "dense", "sparse", "graph_rerank"
```

**Prometheus Metrics** (add to `internal/metrics/metrics_hybrid.go`):
```go
// HybridDenseResultRatio — gauge{dataset} (fraction of top-K results from dense ANN)
// HybridSparseResultRatio — gauge{dataset} (fraction of top-K results from BM25)
// HybridRRFFusionLatencySeconds — histogram{dataset} (RRF rank fusion phase only)
// HybridGraphReRankLatencySeconds — histogram{dataset} (graph re-ranking phase)
// HybridGraphReRankEnabled — counter{dataset, enabled="true|false"} (usage tracking)
// HybridResultOriginTotal — counter{dataset, origin="dense|sparse|graph_expanded"}
//   (per-result provenance for debugging)
```

---

## 📋 REMAINING OPTIMIZATION WORK

### Optimization Status (2026-03-27 Update)

#### ✅ COMPLETED OPTIMIZATIONS

| Optimization | Status | Impact |
|-------------|--------|--------|
| Blocked SIMD for float/int/uint (768+) | ✅ Complete | +30-50% QPS |
| Complex64/128 blocked via cast | ✅ Complete | +20-30% QPS |
| TurboQuant NEON Kernels (FWHT) | ✅ Complete | +3.7x Core / +40% QPS |
| HNSW M=32 for 768+ dims | ✅ Complete | +15-20% QPS |
| Prefetch for 1536+ dims | ✅ Complete | +10-15% QPS |
| Full Audit (18GB, 1k-15k count) | ✅ Complete | Baseline Estab |

#### 📋 REMAINING WORK

| Task | Priority | Est. Effort |
|------|----------|-------------|
| Add search-layer metric sampling | Low | 2 hours |
| Final regression test for full matrix | Med | 8 hours |
| Local buffer pool for high-dim vectors | Med | 4 hours |

---

## Executive Summary

Enable optimized SIMD kernels for all supported dimensions (128-3072) across all data types. This plan addresses performance degradation at high dimensions (≥768) and ensures consistent QPS across the entire supported dimension range.

## Current State Analysis (Updated 2026-03-27)

| Dimension | float32 | float64 | int32 | int16 | int8 | complex64 | turboquant |
|-----------|---------|---------|-------|-------|------|-----------|------------|
| 128 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 256 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 384 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 768 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1024 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1536 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 2048 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 3072 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |

**Legend:**

- ✅ Blocked = Blocked SIMD implementation (256/512 byte blocks)
- ✅ Blocked+Prefetch = Blocked SIMD with prefetch hints (1536+ only)
- ✅ Optimized = Direct SIMD kernels (128-384)

---

## 10-Step Implementation Roadmap (Progress)

### Step 1: Analyze Current SIMD Kernel Implementations

**Status**: ✅ COMPLETE

### Step 2: Benchmark Baseline for All Dimensions/Types

**Status**: ✅ COMPLETE

### Step 3: Implement Blocked SIMD for Missing Dimensions

**Status**: ✅ COMPLETE

### Step 4: Add Type-Specific Optimizations (TurboQuant/Complex)

**Status**: ✅ COMPLETE

### Step 5: Add Unit Tests for All Kernel Variants

**Status**: ✅ COMPLETE

### Step 6: Add Prometheus Metrics for Performance Stability

**Status**: ✅ COMPLETE

### Step 7: Add Memory Pressure Metrics

**Status**: ✅ COMPLETE (Allocation tracking and zero-copy monitoring enabled)

### Step 8: Integrate Metrics into Search Hot Paths (with Sampling)

**Status**: 📋 IN PROGRESS

- Sampling logic for search layer to avoid overhead.

### Step 9: Run Final Performance Matrix

**Status**: 📋 PENDING

### Step 10: Document Results and Final Update

**Status**: 📋 PENDING

---

## 2026-03-27 Optimization Achievements

- **float32/64**: Fully optimized for all dimensions 128-3072 using blocked SIMD + prefetch.
- **int8/16/32/64**: Fully optimized for all dimensions using blocked SIMD.
- **complex64/128**: Fully optimized via zero-copy casting to float paths.
- **turboquant**: Fully optimized with NEON-vectorized rotation and Hadamard kernels.

---

**Last Updated**: 2026-03-28
