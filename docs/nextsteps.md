# Longbow Next Steps & Active Roadmap

This document outlines the active roadmap initiatives, architectural improvements, and validation tasks for upcoming Longbow releases, informed by the 2026-09-11 benchmark results.

---

## 1. Recently Completed

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
- **Results**: +229% complex64 dense at 50k CPU, +191% turboquant4 dense at 50k CPU, +209% float32 graphrag at 500k CPU. Regressions remain for int8/uint8/float32 at small scale.

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

## 2. Active Priority Items (from Benchmark Findings)

| # | Priority | Initiative | Target | Subsystem | Rationale |
|---|:--------:|-----------|:------:|-----------|-----------|
| **1** | **P0** | **emlgo float32 dense 50k CPU regression** | Next release | emlgo / SIMD | -63% QPS regression (792 vs 2116). float32 is the most common float type; this regression blocks emlgo adoption for general workloads. Profile the emlgo dispatch chain to identify overhead. |
| **2** | **P0** | **emlgo int8/uint8 50k CPU regression** | Next release | emlgo / SIMD | -24-34% QPS regression. Most common integer types lose significant performance at small scale. The emlgo dispatch layer adds overhead that dominates for simple dot products. |
| **3** | **P0** | **GPU complex64 sparse 500k regression** | Next release | GPU / emlgo | -80% QPS regression (1319 vs 6652). Severe GPU regression for complex64 sparse search. Root cause likely in CUDA kernel dispatch or memory access patterns. |
| **4** | **P1** | **emlgo conditional dispatch by dtype** | Next release | emlgo / routing | Implement runtime dtype-based dispatch: use emlgo for complex64/complex128/turboquant4, fall back to standard for int8/uint8/float32 at small scale. This captures emlgo's strengths while avoiding regressions. |
| **5** | **P1** | **GPU emlgo complex128 dense 50k** | Next release | GPU / emlgo | -72% QPS regression (935 vs 3358). Consistent across both benchmark runs. Likely a fundamental dispatch issue for complex types on GPU. |
| **6** | **P1** | **CPU emlgo complex128 memory 500k** | Next release | emlgo / memory | +24% peak memory (13387 vs 10787 MB). Investigate additional buffer allocation in emlgo complex128 path. |
| **7** | **P2** | **GPU float16 dense 50k emlgo** | Next release | GPU / emlgo | -45% QPS regression (1889 vs 3427). float16 is important for ML inference workloads. |
| **8** | **P2** | **Benchmark variance investigation** | Next release | benchmarking | Run-to-run QPS variance is high (e.g., CPU standard float16 dense 50k: 1714→3828, +123%). Investigate whether worker count change (6→8) or system load causes instability. Consider running 3x and averaging. |

---

## 3. Future Considerations

### Architecture
- **RDMA Zero-Copy Transport**: Extend Arrow Flight to RoCEv2/InfiniBand for multi-node distributed search.
- **Learned Index Routing**: Adaptive index selection based on query distribution profiling.
- **WebAssembly ONNX Runtime**: Browser-native inference via Wazero WASM runtime.

### Performance
- **emlgo v0.5**: Address the float32/int8/uint8 regressions at small scale. The SIMD kernels are fast but dispatch overhead dominates for simple types on small datasets.
- **GPU emlgo complex type optimization**: The -72-80% regressions for complex types on GPU suggest the CUDA kernel dispatch path needs restructuring.
- **TurboQuant recall validation**: Validate Recall@10 accuracy across all datatypes with adaptive bit-depth (2-bit, 4-bit, 8-bit).
- **Multi-node RDMA validation**: Live cluster validation of RoCE v2 and InfiniBand Arrow Flight RDMA transport.

### Operational
- **emlgo build-tag CI**: Automate emlgo vs standard builds in CI to catch regressions early.
- **Per-dtype performance regression tests**: Add QPS threshold assertions for each dtype to prevent future regressions.
- **Memory budget modeling**: Establish peak memory models per dtype × scale to predict production requirements.
