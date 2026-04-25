# Next Steps for Longbow (Updated 2026-04-25)

## 0.1.9 Release Blockers (RESOLVED)

### CUDA Backend Stability & Performance

- [x] **Top-K Algorithm**: Implemented shared-memory atomic heap Top-K kernel in `kernels.cu`.
- [x] **Memory Isolation**: Refactored `CUDAIndexHandle` to use distinct buffers for FP32, FP16, PQ, and TQ to prevent corruption.
- [x] **PQ Data Integrity**: Implemented `AddPQ` and `cuda_add_vectors_pq` to correctly handle uint8 codes on the device.
- [x] **P0: Hardware-accelerated GraphRAG**: Native traversal of billion-scale knowledge graphs via CUDA/Metal kernels.
- [x] **P0: Adaptive Re-quantization**: Real-time memory and precision optimization based on query load and memory pressure.

### Metal Backend Gaps

- [x] **PQ Implementation**: Fully implemented Metal ADC kernels and PQ interface in `MetalIndexOptimized`.
- [x] **Graph Kernels**: Metal kernels for BFS expansion and activation propagation.

### IVF-HNSW Composite Index

- [x] **Persistence**: Implemented full `Save`/`Load` logic including OPQ encoder and cluster serialization.
- [x] **State Management**: Implemented `ExportState`/`ImportState` for full index synchronization.

### General Hardening

- [x] **TPU Index Safety**: Explicitly flagged `TPUIndex` as an experimental stub.
- [x] **Test Placeholders**: Implemented real recall validation in `DualIndexHarness` and dependency injection tests for `ShardedHNSW`.
- [x] **Namespace Metrics**: Finished implementation of `NamespaceCreationTotal` metrics in `servers.go`.

## Roadmap for 0.2.0

- [ ] **Full Test Coverage (95%)**: Comprehensive hardening of core packages (`store`, `query`, `storage`) to meet enterprise stability standards.
- [ ] **TPU Support**: Production-ready implementation of Google TPU-accelerated indexing and search.
- [ ] **GPU Sharding**: Automated partitioning of billion-scale indices across multiple GPUs.
- [ ] **PiZero & Edge Optimization**: Full completion of low-memory, low-power CPU-only coverage for Raspberry Pi Zero (ARMv6/v7/v8) and other edge devices.
