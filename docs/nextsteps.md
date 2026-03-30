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

## 🎯 Feature Parity — Milvus / Pinecone / Qdrant

Missing features for parity with leading vector databases:

| # | Feature | Milvus | Pinecone | Qdrant | Evidence |
|---|---------|--------|----------|--------|----------|
| 1 | **Multiple index types** (IVF, PQ, DiskANN) | ✅ | ✅ | ✅ | [Milvus README](https://github.com/milvus-io/milvus/blob/master/README.md) |
| 2 | **Full-text / hybrid search** (BM25 + vector) | ✅ | ✅ | ✅ | [Qdrant QUICK_START](https://github.com/qdrant/qdrant/blob/master/docs/QUICK_START.md) |
| 3 | **Multi-tenancy** (namespaces, tenant isolation) | ✅ | ✅ | ✅ | [Milvus RBAC PR](https://github.com/milvus-io/milvus/pull/48197) |
| 4 | **Rich scalar types** (JSON, arrays) | ✅ | ✅ | ✅ | [Milvus Data Fields](https://github.com/milvus-io/milvus) |
| 5 | **Cloud-native managed services** (BYOC, serverless) | ✅ | ✅ | ✅ | [Milvus Zilliz Cloud](https://github.com/milvus-io/milvus) |
| 6 | **Backup / restore snapshots** | ✅ | ✅ | ✅ | [Pinecone Backup](https://docs.pinecone.io/guides/manage-data/back-up-an-index) |
| 7 | **Fine-grained RBAC** | ✅ | ✅ | ✅ | [Milvus RBAC](https://github.com/milvus-io/milvus/pull/48197) |
| 8 | **Dedicated read nodes / replicas** | ✅ | ✅ | ✅ | [Pinecone Read Nodes](https://docs.pinecone.io/guides/index-data/dedicated-read-nodes) |
| 9 | **Enhanced observability** (beyond Prometheus) | ✅ | ✅ | ✅ | [Milvus Monitoring](https://github.com/milvus-io/milvus) |
| 10 | **Tiered storage** (hot/cold, S3 backend) | ✅ | ✅ | ✅ | [Qdrant Storage](https://github.com/qdrant/qdrant/pull/6603) |

---

## ⚡ GPU Performance Improvements — Metal & CUDA

10 optimizations for Metal and CUDA backends across all dtypes:

| # | Improvement | Target Backend | Status | Evidence |
|---|-------------|----------------|--------|----------|
| 1 | **FP16 (half-precision) kernels** | Metal | ✅ Done | `internal/gpu/metal/metal_gpu_optimized.go:269-371` — `compute_l2_distances_fp16`, `compute_cosine_similarity_fp16`, `compute_dot_product_fp16` |
| 2 | **SIMD/warp-level reductions** | Metal | ✅ Done | `internal/gpu/metal/metal_gpu_optimized.go:373-472` — `compute_l2_distances_warp`, `compute_l2_and_topk_warp` using `simd_shuffle_down` |
| 3 | **Multiple index types (IVF, PQ, Flat)** | CUDA | ✅ Done | `internal/gpu/faiss/faiss_gpu.go:26-30` — `FaissIndexFlat`, `FaissIndexIVFFlat`, `FaissIndexIVFPQ` |
| 4 | **GPU-side HNSW refinement** | Hybrid | ⏳ Pending | `internal/store/hnsw_gpu.go:343-366` — CPU refinement fallback |
| 5 | **Adaptive SyncBatchSize** | GPU sync | ⏳ Pending | `internal/store/hnsw_gpu.go:23-28` — default 1000 |
| 6 | **Cross-backend memory pooling** | Both | ⏳ Pending | `internal/gpu/memory/memory_pool.go:37-55` |
| 7 | **Tensor Core paths (FP16/TF32)** | CUDA | ⏳ Pending | FAISS GPU supports FP16, requires Go bindings |
| 8 | **SoA memory layout** | GPU storage | ⏳ Pending | `internal/gpu/metal/metal_gpu.go` — AoS layout |
| 9 | **Mixed-precision compute path** | Both | ⏳ Pending | `docs/performance.md:38-55` — dtype coverage |
| 10 | **Kernel occupancy optimization** | Metal | ✅ Done | `internal/gpu/metal/metal_gpu_optimized.go:131-182` — existing occupancy |
| 11 | **GPU profiling instrumentation** | Metrics | ⏳ Pending | `docs/performance_metal.md:7-15` |

---

## 📋 Backlog (Future Considerations)

### 4. Native Go GPU Kernels — Replace FAISS Dependency

**Status**: ✅ EVALUATED (2026-03-29)

After evaluation, **recommend KEEPING FAISS** for production use. See rationale below.

| Aspect | Current State | Target State |
|--------|---------------|--------------|
| **FAISS** | CGO bindings to `libfaiss` and `libfaiss_gpu` | Keep as-is |
| **CUDA** | Depends on FAISS C++ library | Continue using FAISS |
| **Build** | Requires `faiss` + `cudart` + `cublas` linking | Keep complex build |
| **Cross-compile** | Complex C++ toolchain | Use cross-compile flags |

**Evaluation findings**:

| Option | Status | Notes |
|--------|--------|-------|
| **FAISS (current)** | ✅ Keep | Production-ready, well-tested, full index type support |
| **cuVS (NVIDIA)** | ❌ Not viable | Requires cuDNN/cuVS installation, same C++ complexity |
| **kelindar/search** | ⚠️ Limited | Pure Go, uses llama.cpp, for embedded/small scale |
| **cudago** | ⚠️ Early | Pure Go CUDA, insufficient vector search features |
| **go-cuda-toolkit** | ⚠️ Early | Driver API only, no high-level index abstractions |

**Why keep FAISS**:
1. **Mature**: 20+ years of development, battle-tested in production
2. **Complete**: IVF-Flat, IVF-PQ, HNSW, DiskANN all implemented
3. **Performant**: Hand-tuned CUDA kernels, Tensor Core support
4. **Supported**: Active maintenance by NVIDIA community
5. **Risk**: Replacing would introduce significant integration risk

**Alternative consideration**:
- For **embedded/edge** use cases: Consider `kelindar/search` (pure Go, llama.cpp)
- For **serverless**: Keep FAISS, simplify builds with Docker multi-stage

**Recommendation**: Close as "not pursuing" — maintain FAISS for CUDA backend.

---

- Final regression test for full matrix (Med priority)

---

**Last Updated**: 2026-03-29

All HIGH PRIORITY items complete.
