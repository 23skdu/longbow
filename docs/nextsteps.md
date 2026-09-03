# Longbow Next Steps & Active Roadmap

This document outlines the remaining active roadmap initiatives, architectural improvements, and validation tasks for upcoming Longbow releases (**v0.3.0+**).

Completed release tasks (including the 0.2.3-rc1 P0 blockers, comprehensive 500k CPU/GPU benchmark suite, `GCTuner` emergency rate limiter, fast-fail readiness check, asynchronous pinned CUDA transfers, automatic spill-to-disk paging at 70% RAM threshold, and TurboQuant standardization via `LONGBOW_AUTO_QUANTIZE=1`) have been fully verified, tested with unit/fuzz tests, and merged.

---

## 1. Active Priority Roadmap Initiatives

| # | Initiative | Target | Subsystem | Description |
|---|---|:---:|---|---|
| **1** | **TurboQuant2 50k Recall@10 Validation** | v0.3.0 | Quantization / Accuracy | Execute comprehensive 50k Recall@10 accuracy validation tests across all supported datatypes with adaptive bit-depth (2-bit, 4-bit, 8-bit) and widened search parameters to benchmark recall vs. uncompressed baseline. |
| **2** | **Multi-Node RDMA Integration Validation** | v0.3.0 | Distributed / Network | Perform live cluster validation of RoCE v2 and InfiniBand Arrow Flight RDMA transport across multi-node topologies using NVIDIA GPUDirect RDMA. |

---

## 2. Completed in v0.3.0 Engine Hardening

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

## 3. Extended Roadmap Initiatives (from `docs/roadmap.md`)

### Experimental Fast Math Integration (`feature/emlgo-math`)
- **Objective**: Integrate the `emlgo` math library (https://github.com/23skdu/emlgo) to replace standard math library calls in distance calculation hotspots.
- **Action Item**:
  - Set up feature branch and wrapper interface in `internal/store/index`.
  - A/B benchmark HNSW graph build times, peak memory, and Recall@K accuracy against Go `math` standard library.
