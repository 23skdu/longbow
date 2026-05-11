# Longbow Storage Engine - Future Roadmap

## Performance Optimizations (v0.2.5+)

The following items are remaining research and optimization tasks to further improve throughput and cross-platform parity.

- **AVX-512 VBMI Bitpacking**: Implement 2-bit packing using `VPMULTISHIFTQB` for further throughput gains on modern CPUs.
- **CUDA/Metal PQ Training Parity**: Refine the K-Means training kernels for even larger batch sizes and multi-GPU configurations.

## Future Considerations

- **TPU Physical Hardware Validation**: Once TPU v7x hardware is accessible, replace the current XLA/HBM stubs with actual driver-level calls and perform end-to-end latency benchmarks.
- **Dynamic Kernel Auto-Tuning**: Implement a runtime profiler to automatically select the optimal SIMD kernel based on the specific dimension and data distribution at ingestion time.
