# Observations & Next Steps

Based on the latest comprehensive benchmark tests running across 128/384 dimensions, 10k-500k vectors, and 4 data types using `unified_benchmark.py`:

## Observations

1. **All 13 Search Modes Functional at Scale:** The fresh build successfully scales up to 500,000 vectors across dense, hybrid, sparse, filtered, temporal, and learned index searches without crashes.
2. **Lock-Free / Zero-Alloc Verification:** 
   - Addressed a critical use-after-free deadlock during concurrent ingestion by removing premature `current.Release()` calls in `compareAndSwapDataLocked` (`arrow_hnsw_memory.go`). 
   - Confirmed that Arrow `RecordBatch` retention policies now correctly match reader/writer goroutine life-cycles during atomic graph growth.
   - `SlabArena` successfully provisions off-heap memory, alleviating Go GC pressure even under heavy indexing workloads.
3. **Memory Limits & ResourceExhausted Errors:** 
   - Previous `OOM` and `ResourceExhausted` failures during direct invocation were traced to the default 1GB `MAX_MEMORY` limit being enforced by `GCTuner` and `AdmissionController`. 
   - Proper injection of `LONGBOW_MAX_MEMORY` via environment variables correctly engages the 16GB limit, allowing stable 500k-scale ingestion.
4. **Data Type Performance:** `int8` consistently achieves over 1.8M vec/s ingest rate on CPU with excellent query latencies.
5. **GCTuner Resilience:** `GCTuner` actively adjusts `GOGC` to aggressive levels when memory ratio exceeds 1.7x, proving effective at mitigating peak heap utilization without crashing the server.

## Recommendations

### Implemented

1. **CLI Flag Consistency:** Added CLI flag parsing in `cmd/longbow/flags.go`. Flags like `--max-memory`, `--listen-addr`, `--data-path`, `--log-format`, `--gpu-enabled`, and others now map directly into the `Config` struct and take precedence over `LONGBOW_*` environment variables. Run `longbow --help` for usage.
4. **Automated Continuous Benchmarking:** Split the CI pipeline in `.github/workflows/ci.yml` into separate `validate` and `benchmark-regression` jobs. The benchmark job (`python3 scripts/unified_benchmark.py --ci`) runs the reduced matrix (10k-50k vectors, 128 dims, float32/int8, dense search) with a 15-minute timeout to catch regressions in PRs.

### Still Pending

2. **Disk-Backed Validation:** Continue validating storage overhead and `io_uring` direct I/O performance on NVMe drives at the 1M+ vector scale.
3. **CUDA Execution:** Launch the CUDA test matrix (`LONGBOW_GPU_ENABLED=true`) on the RTX 4060 to capture acceleration benefits across `float32`, `float16`, and `turboquant8` kernels.
