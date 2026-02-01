# I/O Baseline Benchmarks

**Date**: 2026-02-01
**Tool**: `cmd/bench_io`
**Host**: Local Dev Environment

## 1. Sequential Write (Ingestion Simulation)

### Buffered (OS Page Cache)

- **Throughput**: ~3.2 GB/s
- **Latency**: ~1.2 µs/op
- **Observation**: Ingestion is CPU/Memory bound when strictly buffered. This is the optimal "burst" rate.

### Synchronous (fsync per 4KB block)

- **Throghput**: ~1.3 MB/s
- **IOPS**: ~333
- **Latency**: ~3.0 ms/op
- **Observation**: Strict durability per movement is drastically slow.
- **Implication**: We MUST implement Group Commit or asynchronous WAL flushing (e.g., flush every 100ms or 1MB) to bridge the gap between 333 IOPS and 800k IOPS.

## 2. Random Read (Search Simulation)

### Cached (Hot Index)

- **Throughput**: ~5.0 GB/s
- **IOPS**: ~1.2M
- **Observation**: Reading from "warm" OS cache (mmap style) is extremely fast.
- **Implication**: Keeping the active index in RAM (via mmap) is critical. Once dataset size exceeds RAM, we expect performance to drop to disk random seek speeds (likely <10k IOPS for NVMe, <500 IOPS for HDD).

### Mmap Read (Random Access)

- **Throughput**: ~41.7 GB/s
- **IOPS**: ~10.7M
- **Latency**: ~0.1 µs/op
- **Observation**: 8x faster than standard `read()` syscalls for hot data. Directly correlates to memory speed without kernel context switch overhead.
- **Implication**: `DiskGraph` architecture (using mmap) is validated as the correct choice for high-throughput vector search.

### Mmap Scan (Sequential Access)

- **Throughput**: ~41.1 GB/s
- **Observation**: Similar performance to random access when data is fully resident.

## Next Steps

1. Implement **Group Commit** logic in `DeviceWAL` to batch fsyncs.
2. Investigate **Direct I/O** for large dataset traversal to avoid double-caching if RAM is tight.
