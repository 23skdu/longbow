# Next Steps: Performance & Reliability Roadmap

Based on the [0.1.4-rc1] soak test analysis (3-node cluster, 15k vectors, mixed read/write/delete workload), the following 10 steps identify the highest impact areas for improvement.

## 1. Hot-Path Vector Access Optimization

**Problem**: The `pruneConnectionsLocked` loop—the hottest CPU path in HNSW construction—now includes safety checks (`if vec == nil`) to prevent panics. This adds branch prediction pressure.
**Solution**: Guarantee data presence by pre-allocating "Sentinel Vectors" for all referenced IDs during the `AddBatch` phase, or enforce strict transactional boundaries so checks can be removed.
**Expected Impact**: 10-15% reduction in index construction CPU time.

## 2. Static SIMD Dispatch

**Problem**: `simd.EuclideanDistance` performs dynamic dispatch (checking CPU capabilities) or relies on compiler inlining that may not perfectly resolve overhead for every single distance matching call (millions per second).
**Solution**: Use a global function pointer initialized at startup (`var DistFunc = resolveDistFunc()`) to pay the dispatch cost only once, ensuring the inner loop is a direct assembly call.
**Expected Impact**: 5-10% improvement in search latency and indexing speed.

## 3. Binary Search Protocol (DoExchange)

**Problem**: The current `VectorSearch` implementation uses Flight `DoAction` with JSON payloads. JSON serialization/deserialization consumes CPU and bloats network traffic (~2x overhead vs binary).
**Solution**: Migrate the search path to `DoExchange` using a defined Arrow Schema (or internal binary format) for the query vector and parameters.
**Expected Impact**: >20% reduction in query latency, especially for high QPS.

## 4. Parallel Distributed Scatter-Gather

**Problem**: `Coordinator.GlobalSearch` fans out interactions to peers. Optimizing this concurrency model (e.g., using a specialized `errgroup` with bounded concurrency and context propagation) can reduce tail latency (P99).
**Solution**: Refine the scatter-gather logic to use a persistent connection pool with speculative execution (query fastest duplicates) if replication is enabled.
**Expected Impact**: Reduced P99 latency in distributed setups.

## 5. Async DiskStore Reads (io_uring)

**Problem**: While WAL uses `io_uring`, the `DiskStore` (used for larger-than-memory datasets) relies on mmap or standard syscalls which can stall the go runtime thread on page faults.
**Solution**: Implement `io_uring` for random reads in `DiskStore`, allowing the Go runtime to continue scheduling other goroutines during disk I/O.
**Expected Impact**: Higher throughput for disk-resident workloads; unblocked search threads.

## 6. Adaptive Garbage Collection (GOGC)

**Problem**: Soak tests showed ~28k vectors/s ingestion, which generates massive garbage. Static `GOGC` (e.g. 75) might trigger too frequently or too late.
**Solution**: Implement a feedback-loop controller that adjusts `GOGC` dynamically based on allocation rate and free memory, maximizing throughput during ingest and minimizing latency during search.
**Expected Impact**: Smoother latency profile (reduced GC pause outliers).

## 7. Fragmentation-Aware Compaction

**Problem**: Deletion is currently fast (bitset flip), but leaves gaps. Current compaction runs periodically or by batch count.
**Solution**: Track "Tombstone Density" per batch. Trigger compaction specifically for batches where deleted records exceed a threshold (e.g., 20%), optimizing merge efficiency.
**Expected Impact**: Reduced detailed memory usage and improved cache locality.

## 8. HNSW Connectivity Repair Agent

**Problem**: High-concurrency deletions and pruning can theoretically create disconnected sub-graphs ("islands") in HNSW, hurting recall.
**Solution**: Run a background "Repair Agent" that randomly traversing the graph from entry points to ensure reachability, re-linking orphans if found.
**Expected Impact**: Long-term recall stability (99.9%+) without full re-indexing.

## 9. Thread Pinning & NUMA Awareness

**Problem**: Vectors and Graph Data allocated on one NUMA node might be accessed by threads on another, causing QPI/interconnect traffic.
**Solution**: Explicitly pin ingest/search worker pools to specific CPU cores and allocate memory from local nodes where possible (using `unix.Mbind` or similar concepts via CGO/assembly if needed).
**Expected Impact**: Reduced L3 cache misses; linear scaling on high-core-count servers.

## 10. eBPF Network Profiling

**Problem**: "Connection refused" and network saturation issues are hard to debug from application logs alone.
**Solution**: Integrate eBPF hooks (via Cilium or generic tools) to expose TCP window metrics, retransmits, and socket queue depths as Prometheus metrics.
**Expected Impact**: Instant visibility into network bottlenecks and capacity limits.
