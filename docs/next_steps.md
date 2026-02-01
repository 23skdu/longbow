# Longbow Development Roadmap: I/O & Storage Optimization

**Objective**: Maximize system throughput and minimize latency by optimizing the I/O path, storage formats, and memory management strategies.

---

## 🚀 Phase 1: I/O & Throughput Optimization (10-Part Plan)

1. **Baseline I/O Benchmarking** [COMPLETED]
    * **Goal**: Establish a trusted baseline for current disk I/O performance during high-ingestion and concurrent search scenarios.
    * **Tasks**:
        * [x] Create a specialized benchmark suite (`cmd/bench_io`) to measure raw IOPS and throughput for WAL writes and Index reads.
        * [x] Profile current `fsync` patterns and latency distribution.
        * [x] Measure "Time to First Byte" for cold index loading.

2. **Telemetry & I/O Observability** [COMPLETED]
    * **Goal**: Gain deep visibility into storage subsystems to identify bottlenecks in real-time.
    * **Tasks**:
        * [x] Integrate `/proc/diskstats` or equivalent OS metrics into the Prometheus exporter.
        * [x] Trace I/O calls in the WAL and Persistence layers using OpenTelemetry/Prometheus.
        * [x] Add metrics for "dirty pages" and system-level write amplification.

3. **Parallel WAL Archival & Recovery** [COMPLETED]
    * **Goal**: Prevent WAL I/O from blocking the ingestion critical path.
    * **Tasks**:
        * [x] Implement asynchronous/buffered WAL writes with configurable sync intervals (`WALBatcher`).
        * [x] Parallelize WAL segment replay during startup/recovery (`Pipelined ReplayWAL`).
        * [x] Investigate group commit strategies for batched writes (Implemented in `WALBatcher`).

4. **Zero-Copy Memory Mapping (mmap) Optimization** [COMPLETED]
    * **Goal**: Reduce memory overhead and copy costs for large datasets.
    * **Tasks**:
        * [x] Audit `ArrowHNSW` and `DiskGraph` to ensure full zero-copy access where possible (Confirmed).
        * [x] Implement `madvise` hints (e.g., `MADV_RANDOM` vs `MADV_SEQUENTIAL`) based on access patterns (Implemented in `DiskGraph`).
        * [x] Benchmark mmap vs. direct `read()` for different graph sizes (Mmap is 8x faster).

5. **Concurrent & Non-Blocking Checkpointing**
    * **Goal**: Eliminate "stop-the-world" pauses during disk snapshots.
    * **Tasks**:
        * Implement a Copy-On-Write (COW) snapshotting mechanism for the HNSW graph.
        * Refactor `GraphStore` to perform serialization on a background thread without blocking readers or writers.
        * Rate-limit disk flushes to prevent I/O saturation during checkpoints.

6. **Vector Compression & Encoding**
    * **Goal**: Reduce disk footprint and increase effective I/O bandwidth.
    * **Tasks**:
        * Implement block-level compression (Zstd, LZ4) for the `DiskStore`.
        * Evaluate delta-encoding for HNSW adjacency lists (neighbor IDs are often close).
        * Benchmark decompression overhead vs. I/O savings.

7. **Async/Direct I/O (io_uring)**
    * **Goal**: Maximize NVMe utilization and reduce syscall overhead.
    * **Tasks**:
        * Prototype an `io_uring` backend for the `DiskStore` (Linux only).
        * Implement vectored I/O (`readv`/`writev`) for batched vector retrievals.
        * Compare performance against standard Go `os.File` operations.

8. **Fragmentation-Aware Compaction**
    * **Goal**: Maintain sequential I/O patterns over time.
    * **Tasks**:
        * Enhance the `FragmentationTracker` to trigger compaction based on "read amplification" metrics, not just deleted ratio.
        * Implement a "Move-to-Front" or equivalent strategy for frequently accessed hot vectors during compaction.
        * Visualise disk layout fragmentation.

9. **Tiered Storage Policies**
    * **Goal**: optimize cost/performance by moving cold data to cheaper storage.
    * **Tasks**:
        * Implement a default `HotWarm` policy: Keep Index in RAM/NVMe, offload raw Vector content to S3/BlobStore after N days/hours.
        * Implement transparent fetching of "warm" vectors from remote storage during search.
        * Add caching layer (LRU) for remote fetched vectors.

10. **Load Testing & Chaos Validation**
    * **Goal**: Prove reliability under saturation.
    * **Tasks**:
        * Run 24-hour saturation soak tests with induced I/O throttling/latency (using `cgroups` or `tc`).
        * Verify data integrity after power-loss simulations (process kill).
        * Publish final report: "Longbow I/O Performance Characterization".

---

## 📂 Previous Accomplishments

### Native ArrowHNSW Consolidation [COMPLETED]

* **Metric Synchronization**: Verified 100% parity between code and docs.
* **Dependency Removal**: Successfully removed `github.com/coder/hnsw`.
* **Feature Parity**: BQ, PQ, and High-Dim support verified native.
* **Graph Navigator**: Advanced graph traversal support implemented.
