# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-23

---

## ✅ COMPLETED (2026-04-23): Production Hardening (0.1.9)

Successfully finalized the production hardening for the 0.1.9 release, focusing on high-performance compute and memory locality.

- **Specialized FP16 CUDA Kernels**: Implemented shared-memory cached kernels in `internal/gpu/cuda/kernels.cu` to maximize Tensor Core utilization on RTX 40/50 series.
- **Arrow-Native Metadata Transition**: Transitioned from `map[string]interface{}` to a zero-allocation `ArrowMetadata` binary format, eliminating heap fragmentation.
- **NUMA-Local Memory Pinning (HNSW)**: Implemented NUMA-aware shard allocation and thread pinning to reduce cross-socket latency on multi-socket servers like `ancalagon`.
- **AVX-512 Kernels for AMD64**: Modularized and implemented AVX-512 SIMD kernels for L2, Cosine, and Dot Product distances in `internal/simd/avx512.go`.

---

## ✅ ARCHIVED COMPLETED (0.1.9-rc4/5)

- **Metal ONNX Acceleration**: MSL implementation for character-overlap and embedding.
- **Learned Index Hardening**: Live metrics collection and adaptive thresholding.
- **Tokenizer Reliability**: Required `vocab.txt` and removed dummy fallbacks.
- **GPU Detection Accuracy**: Real Metal memory detection on macOS.
- **GraphRAG Indexing Stability**: Resolved deadlocks in `InsertWithVector`.
- **Apache Arrow Zero-Copy**: Optimized `ExtractVectorFromArrow` for O(1) memory access.
- **Performance Benchmarking**: Finalized the full matrix validation and release tagging (0.1.9-rc5).

---

## 🎯 NEXT STEPS (Post-0.1.9)

### 🚀 Performance Optimizations

- [ ] **Dynamic Neighbor Selection**: Implement heuristic-based neighbor pruning during `SearchHybrid` to reduce graph traversal overhead when `alpha < 0.5`.
- [ ] **Lock-Free Search Context**: Transition `ArrowSearchContext` from a pool to a lock-free thread-local storage pattern for lower hot-path latency.
- [ ] **Asynchronous Graph Enrichment**: Move `AddEdge` operations to a dedicated background worker to decouple logical graph updates from physical vector ingestion.

### 🚀 Future Roadmap (0.1.10+)

- [ ] **Google TPU v7x (Ironwood) Support**:
  - **Phase 1: Architecture & Detection**:
    - Implement `TPUDetector` in `internal/gpu/detection.go` for `linux/amd64`.
    - Detect dual-chiplet topology and NUMA affinity (2 NUMA nodes per VM).
  - **Phase 2: XLA-Backed Inference**:
    - Implement `TPUBackend` using OpenXLA/Pallas for custom vector kernels.
    - Optimize for 192GB HBM per chip and multi-tier memory (VMEM/HBM/PCIe).
  - **Phase 3: Observability & Metrics**:
    - Track `longbow_tpu_hbm_usage_bytes` and core utilization.
  - **Phase 4: Verification & Stability**:
    - Unit/Fuzz tests and comparative parity testing.
- [ ] **Cross-Shard Atomic Commits**: Two-phase commit protocol for cross-shard vector updates.
- [ ] **KV-Integrated Indexing**: Native integration with FoundationDB for metadata-heavy searches.

---

## Architecture Notes

### Build Tags - Expected Stubs (NOT Issues)

- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`
