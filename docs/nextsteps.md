# Longbow Next Steps & Recommendations

**Last Updated**: 2026-03-29
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)

---

## 🚨 HIGH PRIORITY — Pending Items

### 1. Native TurboQuant Storage API

**Problem**: Clients cannot explicitly declare `vector_type = "turboquant"` at the API level to opt into the more storage-efficient TurboQuant index path.

**Plan**:
- Add `VectorType` field to `VectorSearchRequest` (Arrow exchange metadata):
  ```text
  vector_type: "float32" | "turboquant" | "int8" | "binary"
  ```
- Create `docs/turboquant_storage.md` documenting:
  - How to create a TurboQuant-indexed dataset: `CreateDataset(vector_type="turboquant", dimension=768)`
  - Wire format: packed `uint8` buffer with TQ header
  - Benchmark comparison: storage size, ingestion QPS, search QPS vs float32

**Unit Tests** (`internal/store/turboquant_storage_test.go`):
```go
// TestCreateDataset_VectorTypeTurboQuant — create dataset with vector_type=turboquant → ok
// TestInsert_TurboQuantVector — insert pre-encoded TQ vector → stored without re-encoding
// TestSearch_TurboQuantDataset — search returns correct neighbours with TQ index
// TestVectorTypeField_PropagatesToSearchOptions — vector_type flows end-to-end
```

**Prometheus Metrics**:
```go
// DatasetVectorTypeTotal — gauge{dataset, vector_type}
// TurboQuantEncodingTotal — counter{dataset, direction="client_provided|server_encoded"}
// TurboQuantStorageBytesTotal — gauge{dataset}
```

---

### 2. Performance Matrix Audit

**Status**: PENDING

Run unified benchmark with:
- Scales: 1k, 10k, 100k
- Dimensions: 128, 768, 1536, 2048
- Backends: CPU, Metal
- Modes: search, hybrid, recommend

Command:
```bash
python scripts/unified_benchmark.py --scales 1k,10k,100k --dims 128,768,1536,2048 --backends cpu,metal --modes search,hybrid,recommend
```

---

### 3. Docker Release v0.1.8-rc1

**Status**: PENDING

- Build and push tags: `0.1.8-rc1` and `latest`
- Registry: `ghcr.io/23skdu/longbow`

---

## ✅ COMPLETED ITEMS (Validated)

### API & Architecture Enhancements

| Feature | Status | Evidence |
|---------|--------|----------|
| **Recommend API** | ✅ DONE | `internal/store/recommend.go`, `recommend_test.go`, `docs/recommendations.md`, Prometheus metrics |
| **EOF Detection** | ✅ DONE | `internal/flight/eof.go`, `eof_test.go`, `IsStreamEOF()` helper |
| **Consistency Levels** | ✅ DONE | `internal/store/consistency.go`, `consistency_test.go`, `SearchOptions.Consistency` field |
| **Dynamic Dimension** | ✅ DONE | `internal/store/dimension.go`, `dimension_test.go`, `DimensionAutoDetected` |
| **GetNeighbors** | ✅ DONE | `internal/store/get_neighbors.go`, `get_neighbors_test.go` |
| **GraphRAG Docs** | ✅ DONE | `docs/graphrag_internals.md` |
| **Hybrid Search Metrics** | ✅ DONE | `internal/metrics/metrics_hybrid.go`, `metrics_hybrid_test.go` |

### v0.1.8-rc1 Release Tasks

| Task | Status | Evidence |
|------|--------|----------|
| HNSW Search Layer fixes | ✅ DONE | `visited.Clear()` in `arrow_hnsw.go:searchLayer` |
| Float64 vector extraction | ✅ DONE | Type-safe parallel extraction in `arrow_search_context.go` |
| HNSW filtering logic | ✅ DONE | Fixed in `arrow_hnsw.go` |
| Store actions linter | ✅ DONE | Fixed `dataType` switch in `store_actions.go` |
| TurboQuant 1536-dim fix | ✅ DONE | Dimension handling in `dataset.go` |
| Performance docs | ✅ DONE | Consolidated in `docs/performance.md` |

### Optimization Status (2026-03-27)

| Optimization | Status | Impact |
|-------------|--------|--------|
| Blocked SIMD for float/int/uint (768+) | ✅ Complete | +30-50% QPS |
| Complex64/128 blocked via cast | ✅ Complete | +20-30% QPS |
| TurboQuant NEON Kernels (FWHT) | ✅ Complete | +3.7x Core / +40% QPS |
| HNSW M=32 for 768+ dims | ✅ Complete | +15-20% QPS |
| Prefetch for 1536+ dims | ✅ Complete | +10-15% QPS |

### SIMD Kernel Status

| Dimension | float32 | float64 | int32 | int16 | int8 | turboquant |
|-----------|---------|---------|-------|-------|------|------------|
| 128 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 256 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 384 | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized | ✅ Optimized |
| 768 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1024 | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 1536 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 2048 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |
| 3072 | ✅ Blocked+Prefetch | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked | ✅ Blocked |

---

## 📋 Backlog (Future Considerations)

- Local buffer pool for high-dim vectors (Med priority)
- Search-layer metric sampling (Low priority)
- Final regression test for full matrix (Med priority)

---

**Last Updated**: 2026-03-29
