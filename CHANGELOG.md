# Changelog

## [0.1.7] - 2026-03-15

### Added
-   **Metal GPU Optimizations**:
    -   SIMD vectorization for L2 distance kernel (4-way float4 parallelism).
    -   Heap-based top-k selection kernel ($O(n \log k)$).
    -   Cosine similarity and dot product Metal kernels.
    -   Batch query support for multiple simultaneous queries.
    -   Unified memory optimization with aligned allocations.
-   **HNSW 'ef' Parameter Tuning**: Dimension-aware `ef` tuning for GPU execution.
-   **ONNX Metal Runtime**: Initial support for ONNX inference on Metal.

### Changed
-   **WAL Replay Optimization**: Increased buffer size and optimized decoded coordination to fix DoPut regressions.
-   **Go Upgrade**: Upgraded to Go 1.26 and updated dependencies.
-   **Security**: Addressed `gosec` G104 issues (error handling in mmap/hash).

### Fixed
-   **WAL**: Deadlock fix and performance optimization plan execution.
-   **Tests**: Fixed `TestWALPerformance` failing on non-Linux platforms (skipped `io_uring` case).
