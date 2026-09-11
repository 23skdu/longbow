# Longbow Next Steps & Active Roadmap

This document outlines the active roadmap initiatives, architectural improvements, and validation tasks for upcoming Longbow releases.

---

## 1. P0 Performance Blockers

### P0-1: Readiness Check Admission Deadlock Prevention (`internal/store/store_actions.go`)
- **Root Cause**: In `store_actions.go:147`, `check_readiness` only queries the admission controller (`CanAdmitSearch()`) if `resp["status"] == "READY"`. When memory utilization exceeds 100% of the physical limit, the ingestion worker throttles indexing to 0. Consequently, `pending` never drops to 0, `check_readiness` stays stuck returning `BUSY` instead of `RESOURCE_EXHAUSTED`, causing clients to block indefinitely up to the 4-hour timeout.
- **Action Item**:
  - In `internal/store/store_actions.go`, evaluate admission control memory pressure *first*, before inspecting queue length.
  - If total physical memory utilization exceeds the emergency threshold (ratio >= 1.0 or >90%), immediately return `status: "RESOURCE_EXHAUSTED"` with `reason: "memory limit exceeded"`.
  - Add unit test verifying that `check_readiness` returns `RESOURCE_EXHAUSTED` under simulated memory pressure even when `pending > 0`.

### P0-2: Cache-Blocked SIMD Graph Traversal on CPU for 64-bit Vectors
- **Root Cause**: On CPU, 8-byte types (`float64`, `int64`, `uint64`) suffer severe L3 cache eviction stalls during HNSW graph descent at scale. Search latency jumped from 2.5-4.5ms @ 200k to **9.35ms @ 500k** and **11.65ms @ 1M** (QPS fell from 1,481 to 552). Meanwhile, GPU maintains **3.13ms @ 500k** and **7.36ms @ 1M** by utilizing high memory bandwidth.
- **Action Item**:
  - Implement cache-blocked SIMD traversal in `internal/store/index/hnsw.go`: batch distance evaluations across candidate neighbors into L1/L2 cache blocks (e.g. 64-vector cache-aligned chunks).
  - Insert software prefetch hints for the next hop's vector coordinates to eliminate L3 memory stalls.

---

## 2. Active Priority Roadmap Initiatives

| # | Initiative | Target | Subsystem | Description |
|---|---|:---:|---|---|
| **1** | **TurboQuant2 50k Recall@10 Validation** | v0.3.0 | Quantization / Accuracy | Execute comprehensive 50k Recall@10 accuracy validation tests across all supported datatypes with adaptive bit-depth (2-bit, 4-bit, 8-bit) and widened search parameters to benchmark recall vs. uncompressed baseline. |
| **2** | **Multi-Node RDMA Integration Validation** | v0.3.0 | Distributed / Network | Perform live cluster validation of RoCE v2 and InfiniBand Arrow Flight RDMA transport across multi-node topologies using NVIDIA GPUDirect RDMA. |

---

## 3. Recently Completed

### EMLGo High-Performance Math Integration (v0.4)
- Integrated [EMLGo](https://github.com/23skdu/emlgo) v0.4.0 as a build-tag-gated SIMD math backend.
- Build with `-tags emlgo` for AVX2/AVX-512/NEON fastmath kernels; default build uses standard Go `math`.
- **1.66x throughput improvement** for hyperbolic tensor operations (Sinh, Cosh, Tanh).
- CPU and GPU Docker images: `Dockerfile.emlgo-cpu`, `Dockerfile.emlgo-gpu`.
- Verified: identical SIMD kernel performance, identical HNSW search profiles between builds.

### Automatic Spill-to-Disk Paging at High Scale
- `LONGBOW_AUTO_SPILL_DISK=1` with `LONGBOW_SPILL_THRESHOLD_RATIO=0.60`.
- Linux cgroup v1/v2 detection and `/proc/meminfo` physical memory detection.
- Zero-copy block-compressed batch appends and transparent HNSW disk extraction.

### TurboQuant Default Storage Engine
- `LONGBOW_AUTO_QUANTIZE=1` as default for 500k+ vector configurations.
- Integrated with `ArrowHNSWConfig`, CLI flags, and runtime `QuantizationTuner`.

### Native Tensor Calculus Engine
- Einstein summation with DAG IR, CSE optimizer, constant folding.
- AVX2 FMA kernels, NVIDIA CUDA cuBLAS kernels.
- Christoffel symbols, Riemann/Ricci curvature, exterior wedge products.
- Prometheus telemetry for all tensor operations.

### Readiness Check Hardening
- `check_readiness` now evaluates memory pressure before queue depth.

### 0.2.3-rc1 P0 Blockers & Infrastructure
- Comprehensive 50k/200k/500k/1M CPU/GPU benchmark suite.
- `GCTuner` emergency rate limiter and fast-fail readiness check.
- Asynchronous pinned CUDA transfers.
- Automatic spill-to-disk paging at 70% RAM threshold.

---

## 4. Future Considerations

- **RDMA Zero-Copy Transport**: Extend Arrow Flight to RoCEv2/InfiniBand for multi-node distributed search.
- **Learned Index Routing**: Adaptive index selection based on query distribution profiling.
- **WebAssembly ONNX Runtime**: Browser-native inference via Wazero WASM runtime.
