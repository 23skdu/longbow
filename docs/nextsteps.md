# Longbow Next Steps & Active Roadmap

This document outlines the active roadmap initiatives, architectural improvements, and validation tasks for upcoming Longbow releases.

---

## 1. Active Priority Roadmap Initiatives

| # | Initiative | Target | Subsystem | Description |
|---|---|:---:|---|---|
| **1** | **TurboQuant2 50k Recall@10 Validation** | v0.3.0 | Quantization / Accuracy | Execute comprehensive 50k Recall@10 accuracy validation tests across all supported datatypes with adaptive bit-depth (2-bit, 4-bit, 8-bit) and widened search parameters to benchmark recall vs. uncompressed baseline. |
| **2** | **Multi-Node RDMA Integration Validation** | v0.3.0 | Distributed / Network | Perform live cluster validation of RoCE v2 and InfiniBand Arrow Flight RDMA transport across multi-node topologies using NVIDIA GPUDirect RDMA. |

---

## 2. Recently Completed

### P0-1: Readiness Check Admission Deadlock Prevention
- **Root Cause**: `check_readiness` only queried the admission controller after checking queue depth. When memory >100%, ingestion is throttled to 0, keeping `pending > 0` forever — causing BUSY instead of RESOURCE_EXHAUSTED.
- **Fix**: `check_readiness` now calls `CanAdmitSearch()` FIRST. If memory pressure exceeds the hard limit, immediately returns `RESOURCE_EXHAUSTED` regardless of queue state.
- **Metric**: `longbow_readiness_exhausted_total` counter tracks occurrences.
- **Test**: `TestCanAdmitSearch_ResourceExhausted_Priority` verifies memory-above-limit returns ResourceExhausted.
- Files: `internal/store/store_actions.go`, `internal/store/admission_test.go`

### P0-2: Cache-Blocked SIMD Graph Traversal (float32)
- **Root Cause**: `searchLayerFloat32` processed all neighbors in one batch, causing L3 cache eviction stalls on 64-bit types at scale.
- **Fix**: Added 64-vector cache-blocked traversal (matching `searchLayerFloat64`): processes neighbors in 64-vector chunks with next-tile prefetching to keep data in L1/L2 cache.
- **Metric**: `longbow_cache_blocked_traversal_chunks_total` counter tracks chunks processed.
- **Test**: `TestCacheBlockedTraversal_MetricIncremented` verifies metric increments during search.
- Files: `internal/store/index/search_float32.go`, `internal/store/index/cache_blocked_traversal_test.go`

### EMLGo High-Performance Math Integration (v0.4)
- Integrated [EMLGo](https://github.com/23skdu/emlgo) v0.4.0 as a build-tag-gated SIMD math backend.
- Build with `-tags emlgo` for AVX2/AVX-512/NEON fastmath kernels; default build uses standard Go `math`.
- CPU and GPU Docker images: `Dockerfile.emlgo-cpu`, `Dockerfile.emlgo-gpu`.

### Automatic Spill-to-Disk Paging at High Scale
- `LONGBOW_AUTO_SPILL_DISK=1` with `LONGBOW_SPILL_THRESHOLD_RATIO=0.60`.
- Linux cgroup v1/v2 detection and `/proc/meminfo` physical memory detection.

### TurboQuant Default Storage Engine
- `LONGBOW_AUTO_QUANTIZE=1` as default for 500k+ vector configurations.

### Native Tensor Calculus Engine
- Einstein summation with DAG IR, CSE optimizer, constant folding.
- AVX2 FMA kernels, NVIDIA CUDA cuBLAS kernels.
- Christoffel symbols, Riemann/Ricci curvature, exterior wedge products.

### 0.2.3-rc1 P0 Blockers & Infrastructure
- Comprehensive 50k/200k/500k/1M CPU/GPU benchmark suite.
- `GCTuner` emergency rate limiter and fast-fail readiness check.
- Asynchronous pinned CUDA transfers.

---

## 3. Future Considerations

- **RDMA Zero-Copy Transport**: Extend Arrow Flight to RoCEv2/InfiniBand for multi-node distributed search.
- **Learned Index Routing**: Adaptive index selection based on query distribution profiling.
- **WebAssembly ONNX Runtime**: Browser-native inference via Wazero WASM runtime.
