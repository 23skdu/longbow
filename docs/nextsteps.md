# Longbow Next Steps & Active Roadmap

This document outlines the active roadmap initiatives, architectural improvements, and validation tasks for upcoming Longbow releases (**v0.3.0+**).

Completed release tasks (including the 0.2.3-rc1 P0 blockers, comprehensive 50k/200k/500k/1M CPU/GPU benchmark suite, `GCTuner` emergency rate limiter, fast-fail readiness check, asynchronous pinned CUDA transfers, automatic spill-to-disk paging at 70% RAM threshold, and TurboQuant standardization via `LONGBOW_AUTO_QUANTIZE=1`) have been verified, tested with unit/fuzz tests, and merged.

---

## 1. P0 Performance Blockers (Identified from Multi-Scale CPU/GPU Evaluation)

The multi-scale benchmark suite (50k, 200k, 500k, 1,000,000 vectors across all 17 datatypes on 10-core CPU and NVIDIA RTX 4060 GPU) identified four critical architectural bottlenecks that must be addressed as **P0 Blockers**:

### P0-1: Readiness Check Admission Deadlock Prevention (`internal/store/store_actions.go`)
- **Root Cause**: In `store_actions.go:147`, `check_readiness` only queries the admission controller (`CanAdmitSearch()`) if `resp["status"] == "READY"`. When memory utilization exceeds 100% of the physical limit (as observed when `complex128 @ 1M` reached 132% / 25.5 GB), the ingestion worker throttles indexing to 0 (`ingestion_worker.go:73`: `"High memory pressure detected, throttling ingestion worker"`). Consequently, `pending` never drops to 0, `check_readiness` stays stuck returning `BUSY` instead of `RESOURCE_EXHAUSTED`, causing clients to block indefinitely up to the 4-hour timeout.
- **Action Item**:
  - In `internal/store/store_actions.go`, evaluate admission control memory pressure *first*, before inspecting queue length.
  - If total physical memory utilization exceeds the emergency threshold (ratio $\ge 1.0$ or $>90\%$), immediately return `status: "RESOURCE_EXHAUSTED"` with `reason: "memory limit exceeded"`.
  - Add unit test verifying that `check_readiness` returns `RESOURCE_EXHAUSTED` under simulated memory pressure even when `pending > 0`.

### P0-2: Automatic Spill-to-Disk Paging for Vectors $\ge 1\text{KB}$ (`complex128`)
- **Root Cause**: At 1,000,000 vectors with 128 dimensions (2,048 bytes per vector for `complex128`), raw vector storage is 2.05 GB, but Arrow record batch allocations, slab pools, and multi-layer HNSW graphs scale off-heap memory to **21.0 GB**. This exceeds available RAM on standard nodes ($\le 24\text{GB}$) and exhausts memory budgets.
- **Action Item**:
  - Enhance `LONGBOW_AUTO_SPILL_DISK=1` threshold calculation to factor in vector element size: automatically force disk-backed vector storage (`DiskVectorStore` / mmap partitions) when `dim * bytes_per_element * scale >= 0.5 * RAM_BUDGET`.
  - For `complex128`, default to disk spillover for scales $\ge 500\text{k}$.

### P0-3: Cache-Blocked SIMD Graph Traversal on CPU for 64-bit Vectors
- **Root Cause**: On CPU, 8-byte types (`float64`, `int64`, `uint64`) suffer severe L3 cache eviction stalls during HNSW graph descent at scale. Search latency jumped from 2.5–4.5ms @ 200k to **9.35ms @ 500k** and **11.65ms @ 1M** (QPS fell from 1,481 to 552). Meanwhile, GPU maintains **3.13ms @ 500k** and **7.36ms @ 1M** by utilizing high memory bandwidth.
- **Action Item**:
  - Implement cache-blocked SIMD traversal in `internal/store/index/hnsw.go`: batch distance evaluations across candidate neighbors into L1/L2 cache blocks (e.g. 64-vector cache-aligned chunks).
  - Insert software prefetch hints (`_mm_prefetch`) for the next hop's vector coordinates to eliminate L3 memory stalls.

### P0-4: VRAM-Aware Automatic Quantization on Consumer GPUs ($\le 8\text{GB}$ VRAM)
- **Root Cause**: On GPUs with 8 GB VRAM (e.g. NVIDIA RTX 4060 Laptop GPU `sm_89`), uncompressed 64-bit vectors (`float64`, `complex64`) at 1,000,000 scale hit device VRAM allocation limits and triggered early aborts, while `float16` and `TurboQuant` achieved **2,143–3,188 QPS** seamlessly.
- **Action Item**:
  - In `cmd/longbow/main.go` and `internal/store/quantization_tuner.go`, inspect CUDA device total VRAM (`cudaGetDeviceProperties`).
  - If VRAM $\le 8\text{GB}$ and collection scale is configured for $\ge 500\text{k}$ vectors with $\ge 4\text{-byte}$ width, automatically enable TurboQuant 4-bit (`LONGBOW_AUTO_QUANTIZE=1`) or downcast to `float16` for CUDA index storage unless explicitly overridden.

---
## 2. Active Priority Roadmap Initiatives

| # | Initiative | Target | Subsystem | Description |
|---|---|:---:|---|---|
| **1** | **TurboQuant2 50k Recall@10 Validation** | v0.3.0 | Quantization / Accuracy | Execute comprehensive 50k Recall@10 accuracy validation tests across all supported datatypes with adaptive bit-depth (2-bit, 4-bit, 8-bit) and widened search parameters to benchmark recall vs. uncompressed baseline. |
| **2** | **Multi-Node RDMA Integration Validation** | v0.3.0 | Distributed / Network | Perform live cluster validation of RoCE v2 and InfiniBand Arrow Flight RDMA transport across multi-node topologies using NVIDIA GPUDirect RDMA. |

---

## 3. Completed in v0.3.0 Engine Hardening

### A. Automatic Spill-to-Disk Paging at High Scale (`LONGBOW_AUTO_SPILL_DISK=1`)
- Automatic fallback to disk backing (`DiskVectorStore` / memory-mapped vector partitions) when vector count and index dimensions exceed 70% of physical RAM (`LONGBOW_SPILL_THRESHOLD_RATIO=0.70`).
- Linux cgroup v1/v2 limit and `/proc/meminfo` total physical memory detection with `LONGBOW_PHYSICAL_RAM` override.
- Zero-copy block-compressed batch appends and transparent HNSW disk extraction.
- Prometheus counter `longbow_auto_spill_to_disk_engaged_total`.

### B. TurboQuant Default Storage Engine Configuration (`LONGBOW_AUTO_QUANTIZE=1`)
- Standardized TurboQuant (default 4-bit, configurable to 2 or 8) as default storage mode for 500k+ vector configurations on memory-constrained infrastructure (≤64 GB RAM) or when uncompressed memory exceeds 30% of physical RAM.
- Integrated with `ArrowHNSWConfig`, CLI flags (`--auto-quantize`, `--auto-quantize-threshold`, `--auto-quantize-bits`), and runtime `QuantizationTuner`.
- Prometheus counter `longbow_auto_quantize_engaged_total`.

### C. Native Tensor Calculus Engine (see [docs/tensor_engine.md](tensor_engine.md))
- **Phase 1: Tensor IR & Einstein Notation Parser**: Implemented high-level `Einsum` with diagonal extraction (`"ii->i"`), trace (`"ii->"`), contraction chains, and DAG IR (`Contract`, `Transpose`, `Reshape`, `Elementwise`, `Reduce`).
- **Phase 2: Index Rewriting Optimizer**: Dynamic programming contraction order optimization (`OptimizePath`), Common Subexpression Elimination (CSE), constant folding, and algebraic simplification ($A \cdot 0 \to 0$, $A + 0 \to A$, $-(-A) \to A$, $T(T(A)) \to A$).
- **Phase 3: Hardware Kernels & Tensor Calculus Intrinsics**: AVX2 FMA kernels (`gemm_amd64.s`), NVIDIA CUDA cuBLAS kernels (`cublasSgemm` & `cublasDgemm`), Levi-Civita permutation symbols ($\epsilon_{i_1 \dots i_n}$), metric inversion & index raising/lowering ($g^{\mu\nu}$), Christoffel connection symbols ($\Gamma^\sigma_{\mu\nu}$), Riemann curvature ($R^\rho_{\sigma\mu\nu}$), Ricci tensor ($R_{\sigma\nu}$), and exterior differential wedge product ($A \wedge B$).
- **Phase 4: Multi-Dtype Execution & Telemetry**: Generic multi-dtype fallbacks (`Float32`, `Float64`, `Complex64`, `Complex128`, `Int32`, `Int64`), zero-copy tensor slicing, and Prometheus metrics (`longbow_tensor_operations_total`, `longbow_tensor_operation_duration_seconds`, `longbow_tensor_bytes_processed_total`, `longbow_tensor_optimizer_passes_total`, `longbow_tensor_optimizer_flops_saved_total`).

---

## 4. Extended Roadmap Initiatives (from `docs/roadmap.md`)

### Experimental Fast Math Integration (`feature/emlgo-math`)
- **Objective**: Integrate the `emlgo` math library (https://github.com/23skdu/emlgo) to replace standard math library calls in distance calculation hotspots.
- **Action Item**:
  - Set up feature branch and wrapper interface in `internal/store/index`.
  - A/B benchmark HNSW graph build times, peak memory, and Recall@K accuracy against Go `math` standard library.
