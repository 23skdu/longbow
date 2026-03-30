# Longbow Next Steps & Recommendations

**Last Updated**: 2026-03-29
**Platform**: Apple M3 Pro (Bahamut), macOS ARM64 + Linux (Ancalagon)

---

## 🚨 HIGH PRIORITY — Pending Items

### 0. Stub & Incomplete Code Fixes ✅ DONE

**Status**: COMPLETE (2026-03-29)

Fixed both incomplete stubs:

| Fix | Status | Evidence |
|-----|--------|----------|
| `GetGPURequirements()` | ✅ Done | Now detects actual GPU availability for CUDA/Metal/OpenCL |
| `stubMLModel.Score()` | ✅ Done | Now uses keyword matching (0.3-0.9 scores) instead of hardcoded 0.5 |

---

### 1. Native TurboQuant Storage API ✅ DONE

**Status**: COMPLETE (2026-03-29)

The TurboQuant storage API is fully implemented:

| Component | Status | Evidence |
|-----------|--------|----------|
| API VectorType field | ✅ Done | `internal/query/requests.go:18-20` - `VectorType` and `TurboQuantBits` fields |
| Unit Tests | ✅ Done | `internal/store/turboquant_storage_test.go`, `turboquant_test.go` |
| Documentation | ✅ Done | `docs/turboquant.md` (181 lines) |
| Prometheus Metrics | ✅ Done | `internal/metrics/storage_metrics.go:750+` |

**Usage**:
```json
{
  "type": "create_dataset",
  "body": {
    "name": "my_tq_dataset",
    "dimension": 768,
    "vector_type": "turboquant",
    "turboquant_bits": 8
  }
}
```

---

### 2. Performance Matrix Audit ✅ DONE

**Status**: COMPLETE (2026-03-29)

Ran unified benchmarks and generated performance docs:

| Doc | Description |
|-----|-------------|
| `docs/performance.md` | CPU performance summary |
| `docs/performance_metal.md` | Metal GPU benchmarks |
| `docs/performance_ancalagon.md` | Ancalagon (Linux) benchmarks |

Benchmarks covered: float32, float64, float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant at dimensions 128-3072.

---

### 3. Docker Release v0.1.8-rc1 ✅ DONE

**Status**: COMPLETE (2026-03-29)

- Release tag: `0.1.8-rc1`
- Built and pushed to: `ghcr.io/23skdu/longbow`

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
