# Auto-Sharding in Longbow

Longbow provides an **Auto-Sharding** mechanism designed to transparently scale vector indices as datasets grow.
This document details how it works, how it interacts with the underlying HNSW index, and how to configure it.

## Overview

By default, Longbow stores vectors in a single monolithic HNSW index (either memory-mapped or in-memory
using Arrow). While efficient for small-to-medium datasets, HNSW build times and memory usage per node
can spiral as the graph grows.

**Auto-Sharding** monitors the size of your index. When the number of vectors exceeds a configured
`threshold`, Longbow automatically migrates the data from the single monolithic index into a
**Sharded Index** (a collection of smaller, independent HNSW graphs), managed seamlessly behind the same API.

## How It Works

The migration process is "transparent," meaning it happens online while the system (read-only or
read-write) remains available, though with some performance caveats.

1. **Detection**: After every write (`Add` or `DoPut`), the system checks if `CurrentIndex.Len() >= AUTO_SHARDING_THRESHOLD`.
2. **Trigger**: If the threshold is breached, a background goroutine (`migrateToSharded`) is spawned.
3. **Dual-Index State**:
    * A new empty `ShardedHNSW` index is created.
    * This new index becomes the `interimIndex`.
    * All *new* writes are directed to the `interimIndex` (or buffered/split).
    * Reads (Search) query *both* the old `current` index and the new `interimIndex` and merge results.
4. **Migration (The Heavy Lift)**:
    * The background worker iterates over every vector in the old index.
    * It re-inserts them into the new Sharded Index (re-calculating HNSW links).
    * **Critical Performance Note**: During this phase, **memory usage effectively doubles**, as both the
      old index and the growing new index reside in memory. CPU usage also spikes due to re-indexing.
5. **Swap & Cleanup**:
    * Once all data is moved, the system atomically swaps the `ShardedHNSW` to be the primary `current` index.
    * The old monolithic HNSW is closed and its resources released.

## Interaction with HNSW

Longbow uses a custom Arrow-based HNSW implementation.

* **Monolithic HNSW**: A standard hierarchical navigable small world graph. Fast for search,
  but single-threaded insert performance degrades at scale, and resizing requires copying data.
* **Sharded HNSW**: Composed of multiple HNSW sub-indices (shards).
  * **Writes**: Distributed across shards (Round-Robin or Ring Hash), allowing higher concurrent write throughput.
  * **Reads**: Scatter-gather search. Queries are executed on all shards in parallel,
    and results are merged (MapReduce style).

## Configuration

Auto-sharding is configured via environment variables or Helm values.

### Helm Configuration (`values.yaml`)

```yaml
sharding:
  auto:
    enabled: false  # Disabled by default (Recommended for benchmarks/small memory)
    threshold: 10000 # Vectors before sharding triggers
    splitThreshold: 65536 # Target size per shard
```

### Environment Variables

* `LONGBOW_AUTO_SHARDING_ENABLED`: `true` or `false`
* `LONGBOW_AUTO_SHARDING_THRESHOLD`: Integer (e.g., `500000`)
* `LONGBOW_AUTO_SHARDING_SPLIT_THRESHOLD`: Integer (e.g., `100000`)

## Performance Implications & Recommendations

### Memory Spike

The most significant side effect of auto-sharding is the **migration memory spike**.
If your node has 4GB of RAM and your dataset takes 2.5GB, triggering auto-sharding will likely cause an
**OOM Kill** because the system attempts to allocate a second 2.5GB index during migration.

**Recommendation**: Ensure your pods have at least **2.5x the memory** of your `AUTO_SHARDING_THRESHOLD`
dataset size if you enable this feature.

### Benchmarking

For benchmarking (e.g., insertion speed tests), it is often better to **disable auto-sharding**
(`enabled: false`) to measure raw HNSW performance without the migration overhead interfering with results.

### Production

For production with growing datasets:

1. **Enable Auto-Sharding**: `enabled: true`.
2. **Tune Threshold**: Set `threshold` to a safe value (e.g., `500000` or `1000000`) depending on your
   node size, so migration happens rarely and only when necessary.
