# Roadmap: Polymorphic Vector Index & 3072-Dim Support

This roadmap details the 12 steps required to generalize the `ArrowHNSW` index to support 12 different data types (Int8...Complex128) and optimize for high-dimensional vectors (up to 3072).

## Step 1: Polymorphic Vector Type System

Introduce a formal type system to identify and validate vector contents.

- **Subtasks**:
  - Define `VectorDataType` enum in `internal/store/index.go`.
  - Update `SearchOptions` and `SearchResult` to handle polymorphic buffers.
  - Implement type-validation logic in `NewArrowHNSW`.
- **Testing**:
  - Unit: Type serialization and error handling for invalid types.
  - Fuzz: Recursive parsing of complex configuration schemas.
- **Metrics**: `ahnsw_index_type_count` (Counter per type).

## Step 2: Generalized GraphData Memory Management

Refactor the internal storage to handle variable-width elements dynamically.

- **Subtasks**:
  - Implement type-parameterized arenas in `GraphData`.
  - Update chunk offset calculations to use `ElementSize` from Step 1.
  - Add support for tiered memory allocation per type.
- **Testing**:
  - Unit: Alignment verification for all 12 types.
  - Fuzz: Stress test rapid allocation under memory pressure.
- **Metrics**: `ahnsw_arena_allocation_bytes_total` (Counter per type).

## Step 3: Expanded Arrow Extraction Logic

Ensure data can flow from any Arrow RecordBatch into the index regardless of precision.

- **Subtasks**:
  - Refactor `ExtractVectorFromArrow` for all integral and floating-point types.
  - Implement optimized zero-copy paths for aligned Arrow buffers.
- **Testing**:
  - Unit: Extraction accuracy from `FixedSizeList` of all types.
  - Fuzz: Random Arrow schema generation with varying column orders.
- **Metrics**: `ahnsw_arrow_extraction_errors_total` (Counter).

## Step 4: Redesign SIMD Dispatcher Framework

Move beyond static dispatch to a dynamic registry capable of handling the type/dimension matrix.

- **Subtasks**:
  - Create a kernel registry in `internal/simd`.
  - Implement multi-key dispatching (Metric, Type, Dimension).
- **Testing**:
  - Unit: Dispatch consistency (ensure the right kernel is called).
  - Fuzz: Dispatcher routing with large/odd dimensions.
- **Metrics**: `ahnsw_simd_dispatch_latency_seconds` (Histogram).

## Step 5: Optimized Baseline Kernels

Provide high-performance Go-fallback kernels for all types to ensure universal compatibility.

- **Subtasks**:
  - Implement unrolled (4x/8x) Go kernels for all 12 types.
  - Optimize for Go compiler auto-vectorization.
- **Testing**:
  - Unit: Numerical correctness checks against reference scalar implementation.
  - Fuzz: Overflow/Underflow stability with extreme integer ranges.
- **Metrics**: `ahnsw_kernel_execution_total` (Counter per type).

## Step 6: 128-3072 Dimension Layout Optimization

Specialized optimizations for the requested dimension power-points.

- **Subtasks**:
  - Implement cache-line padding for specific widths.
  - Add block-based SIMD processing for vectors > 1024 dims.
- **Testing**:
  - Unit: Throughput benchmarks for 128, 384, 768, 1536, 3072 dims.
  - Fuzz: Memory boundary checks for odd dimensions.
- **Metrics**: `ahnsw_search_throughput_dims` (Gauge).

## Step 7: Complex Number Support

Native support for Complex64 and Complex128 vector indexing.

- **Subtasks**:
  - Implement Euclidean distance for complex vectors (L2 of real+imag parts).
  - Update indexing paths to treat complex values as paired floats.
- **Testing**:
  - Unit: Distance accuracy for complex search queries.
  - Fuzz: Stability with large magnitude complex numbers.
- **Metrics**: `ahnsw_complex_ops_total` (Counter).

## Step 8: Refactor HNSW search-and-insert for polymorphism

Decouple the graph traversal logic from the underlying float32 assumptions.

- **Subtasks**:
  - Refactor `mustGetVectorFromData` to use generic views.
  - Update `Search` and `Insert` to use the dynamic dispatcher.
- **Testing**:
  - Unit: End-to-end flow validation for non-float32 types (e.g., Int8 search).
  - Fuzz: HNSW graph connectivity stability with mixed precision insertions.
- **Metrics**: `ahnsw_polymorphic_search_count` (Counter).

## Step 9: Enhanced Observability

Expose the matrix of performance across all types and dimensions.

- **Subtasks**:
  - Add granular Prometheus metrics for per-type latency.
  - Implement dimension-bucketed performance tracking.
- **Testing**:
  - Unit: Metric registration validation.
  - Fuzz: High-frequency metric updates under thread contention.
- **Metrics**: Full Prometheus registry exposure.

## Step 10: Multi-Type Fuzz Testing Suite

A dedicated harness for identifying edge cases in polymorphic ingestion.

- **Subtasks**:
  - Create randomized ingestion fuzzer covering all 12 types.
  - Implement persistence-roundtrip fuzzing.
- **Testing**:
  - Fuzz: Run long-soak fuzzers for 30+ minutes.
- **Metrics**: `ahnsw_fuzz_crash_recovered_total` (Counter).

## Step 11: High-Dimension Growth Stability

Ensure the system remains stable and fast during growth of extremely large vectors.

- **Subtasks**:
  - Optimize `Grow` and `Clone` for 3072-dim vector pre-allocation.
  - Implement slab-aware memory reclamation for large vectors.
- **Testing**:
  - Unit: Index growth from 0 to 10M 3072-dim vectors.
  - Fuzz: Concurrent growth triggered by multiple types.
- **Metrics**: `ahnsw_index_growth_duration_seconds` (Histogram).

## Step 12: Final Integration & Regression

Address legacy test technical debt and perform final validation.

- **Subtasks**:
  - Fix dimension mismatches in existing HNSW tests.
  - Final sharded/multi-node validation across all types.
- **Testing**:
  - Unit: Pass full project test suite with `-race`.
  - Fuzz: Cross-architecture consistency fuzzing.
- **Metrics**: `ahnsw_total_vectors_indexed` (Counter).
