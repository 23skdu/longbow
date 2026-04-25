# Next Steps for Longbow Performance Optimization

## Observations from 0.1.9 Benchmarks

1. **Metal vs CUDA**: On small datasets (500 vectors), local CPU (ARM64) often outperforms remote CUDA (x86_64) due to PCIe transfer overhead.
2. **Type Parity**: Numerical parity has been verified across all 14 data types.
3. **Geo/Temporal Fix**: Resolved a critical bug in `bench-tool` where dataset names were hardcoded to `bench_go`, causing failures in automated matrix runs.
4. **TurboQuant Efficiency**: TurboQuant shows ~1.5x throughput improvement on local Metal backends for dense searches.

## Recommendations

1. **Kernel Fusing**: Implement fused kernels for filtered search to avoid multiple passes over the vector data.
2. **NUMA Pinning**: On high-core count servers like Ancalagon, implement explicit NUMA pinning for the ingestion workers.
3. **Async PCIe Transfers**: For CUDA backends, implement double-buffering or async streams to hide the cost of host-to-device transfers.
4. **Vectorized Metadata**: Transition the metadata filtering engine to use Arrow-native SIMD kernels (already in progress).

## Roadmap for 0.1.10

- [ ] Support for billion-scale IVF-HNSW composite indexing.
- [ ] Integrated Optimized Product Quantization (OPQ) training.
- [ ] Real-time index adaptation based on query patterns.
- [ ] Zero-copy Arrow Flight streaming for all search modes.
