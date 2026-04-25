# Next Steps for Longbow Performance Optimization

## Observations from 0.1.9 Benchmarks

1. **Metal vs CUDA**: On small datasets (500 vectors), local CPU (ARM64) often outperforms remote CUDA (x86_64) due to PCIe transfer overhead.
2. **Type Parity**: Numerical parity has been verified across all 14 data types.
3. **Geo/Temporal Fix**: Resolved a critical bug in `bench-tool` where dataset names were hardcoded to `bench_go`, causing failures in automated matrix runs.
4. **TurboQuant Efficiency**: TurboQuant shows ~1.5x throughput improvement on local Metal backends for dense searches.

## Recommendations (Completed in 0.1.9)
1. **Kernel Fusing**: Implemented fused kernels for filtered search in `kernels.cu`.
2. **NUMA Pinning**: Implemented explicit NUMA pinning for ingestion and indexing workers.
3. **Async PCIe Transfers**: Implemented double-buffering with async streams in `cuda_index.go`.
4. **Vectorized Metadata**: Transitioned the metadata filtering engine to use Arrow-native SIMD kernels.

## Roadmap for 0.1.10
- [x] Support for billion-scale IVF-HNSW composite indexing. (v0.1.9)
- [x] Integrated Optimized Product Quantization (OPQ) training. (v0.1.9)
- [x] Real-time index adaptation based on query patterns. (v0.1.9)
- [x] Zero-copy Arrow Flight streaming for all search modes. (v0.1.9)
- [ ] Multi-GPU sharding support for massive clusters.
- [ ] Dynamic quantization (Int8/Float16) auto-tuning.
- [ ] Distributed consensus for index metadata (Raft integration).
