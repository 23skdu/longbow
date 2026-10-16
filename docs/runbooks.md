# Longbow Operational Runbooks

This guide provides operational triage procedures, diagnosis steps, and mitigation strategies for alerts defined in `grafana/rules.yml`.

---

## 1. Triage Workflow

When an alert fires in Longbow:
1. Identify the alert severity (`critical`, `warning`, `info`).
2. Inspect the dashboard associated with the alert domain (`search-query.json`, `index-storage.json`, `distributed-cluster.json`, `gpu-onnx-health.json`, or `temporal-realtime.json`).
3. Check the affected nodes or datasets using label matching (`dataset`, `node`, `device`, `component`).
4. Follow the specific remediation section below.

---

## 2. WAL & Durability Alerts

### LongbowWALFlushErrors & LongbowWALDirectWriteErrors
- **Severity**: Critical
- **Symptoms**: WAL buffer flush operations or direct writes to disk fail. Ingestion requests may be blocked or fail to commit.
- **Diagnosis**:
  - Check disk space and mount point permissions: `df -h /data`.
  - Check disk health / dmesg for I/O errors.
  - Review server logs for filesystem write failures or io_uring ring completion errors: `journalctl -u longbow | grep -E "WAL|uring|flush"`.
- **Mitigation**:
  - If disk is full, expand the volume or trigger dataset snapshot/compaction to reclaim space.
  - If io_uring errors are logged, restart Longbow with `LONGBOW_STORAGE_USE_DIRECT_IO=false`.

### LongbowCheckpointTimeouts
- **Severity**: Critical
- **Symptoms**: Asynchronous WAL checkpoint operations are timing out.
- **Diagnosis**:
  - High disk write saturation: check `longbow_io_write_latency_seconds`.
  - Check if parallel snapshot generation or compaction is saturating I/O bandwidth.
- **Mitigation**:
  - Increase `LONGBOW_STORAGE_FSYNC_TIMEOUT` if storage device latency has transient spikes.
  - Stagger snapshot intervals (`LONGBOW_SNAPSHOT_INTERVAL`).

### LongbowHighWALQueueDepth & LongbowSlowWALFsync
- **Severity**: Warning
- **Symptoms**: Uncommitted entries in the WAL queue are growing; fsync operations exceed 500ms.
- **Mitigation**:
  - Verify that storage write caching is enabled on the underlying disk.
  - Switch to direct I/O or fast NVMe volumes for `/data`.

---

## 3. Replication, Mesh & Cluster Alerts

### LongbowSplitBrain & LongbowMemberFailure
- **Severity**: Critical
- **Symptoms**: Cluster node membership dropped below quorum. Partition risk detected.
- **Diagnosis**:
  - Check node reachability: `ping`, `traceroute`, or mesh gossip port `3001`.
  - Review network metrics `longbow_gossip_pings_total{direction="failed"}`.
- **Mitigation**:
  - Check container / VM network connectivity.
  - If a network partition occurred, the minority partition will fence itself automatically (`longbow_split_brain_fenced_state`). Restore network routing between nodes.

### LongbowReplicationQueueDropped & LongbowReplicationStalled
- **Severity**: Warning
- **Symptoms**: Outbound replication queue is full and dropping replication packets.
- **Mitigation**:
  - Check replication lag on target follower nodes: `longbow_replication_lag_seconds`.
  - Increase replication buffer queue size or bandwidth between cluster replicas.

### LongbowQuorumFailures
- **Severity**: Critical
- **Symptoms**: Put operations failing to reach required quorum consistency.
- **Mitigation**:
  - Verify follower node health and readiness.
  - Adjust consistency requirements if running under degraded node count.

---

## 4. Indexing & Graph Quality Alerts

### LongbowIndexJobsDropped
- **Severity**: Critical
- **Symptoms**: Ingestion worker queue is overflowing, dropping asynchronous indexing tasks.
- **Mitigation**:
  - Scale up indexing worker concurrency (`LONGBOW_INDEX_WORKERS`).
  - Scale out ingestion across more cluster shards or increase CPU allocation.

### LongbowBulkInsertDimensionErrors & LongbowNeighborSelectionErrors
- **Severity**: Warning
- **Symptoms**: Vectors rejected due to dimensionality mismatch or errors while establishing graph adjacency links.
- **Diagnosis**:
  - Check client vector dimensionality against dataset schema (`longbow_bulk_insert_dimension_errors_total`).
- **Mitigation**:
  - Reject malformed client records before sending batch payloads.

### LongbowLowHNSWAverageDegree & LongbowBulkInsertFasterThanFloat32
- **Severity**: Critical
- **Symptoms**: Graph connectivity is defective, leading to low search recall or isolated nodes.
- **Diagnosis**:
  - Inspect `longbow_hnsw_average_degree{level="0"}`. Healthy builds maintain close to target MMax0.
  - Check whether `LONGBOW_HNSW_BULK_CHAIN_LINKS` is set to 1.
- **Mitigation**:
  - Trigger index repair (`POST /api/v1/datasets/{name}/repair`) or rebuild the graph.

---

## 5. Memory, GC & Eviction Alerts

### LongbowMemoryLimitRejections & LongbowHighBackpressureRejections
- **Severity**: Critical
- **Symptoms**: System memory limit reached; incoming writes or queries are being rejected with backpressure.
- **Diagnosis**:
  - Check `longbow_memory_pressure_level` and resident set size (RSS).
  - Verify if off-heap arenas or cache allocations dominate memory.
- **Mitigation**:
  - Increase container memory limit or decrease cache size (`LONGBOW_TEMPORAL_CACHE_SIZE`).
  - Enable layer eviction: `LONGBOW_LAYER_EVICTION_ENABLED=true` with threshold `0.75`.

### LongbowProcessPanics
- **Severity**: Critical
- **Symptoms**: A panic was recovered in an internal engine component (`longbow_panic_total`).
- **Diagnosis**:
  - Check application logs for stack traces matching the component label.
  - Inspect recent vector schema evolution, query patterns, or JIT kernels.
- **Mitigation**:
  - Isolate the failing query or dataset. File bug report with the panic trace.

---

## 6. GPU & Acceleration Alerts

### LongbowMultiGPUDeviceErrors & LongbowGPUFailure
- **Severity**: Critical
- **Symptoms**: GPU hardware errors reported; vector searches falling back to CPU.
- **Diagnosis**:
  - Run `nvidia-smi` on the host to check for Xid errors, PCIe errors, or overheating.
  - Review `longbow_gpu_device_temperature_celsius` and memory allocation.
- **Mitigation**:
  - If GPU OOM occurs, decrease batch size or allocate more VRAM.
  - If hardware bus error occurred, drain node traffic and restart the host.

### LongbowJITKernelErrors
- **Severity**: Critical
- **Symptoms**: Vector math JIT compilation or execution failure.
- **Diagnosis**:
  - Check CPU instruction capabilities and kernel logs.
- **Mitigation**:
  - Longbow automatically falls back to scalar/AVX generic kernels. Check for memory permission issues on executable pages (W^X / mprotect).

---

## 7. Storage & Compaction Alerts

### LongbowDatasetExportFailures & LongbowDatasetImportFailures
- **Severity**: Warning
- **Symptoms**: Snapshot export/import operations failing for a dataset.
- **Mitigation**:
  - Check S3 / Cloud Storage credentials and endpoint connectivity.
  - Verify local snapshot directory permissions (`LONGBOW_SNAPSHOT_PATH`).

### LongbowStoreDroppedDatasets
- **Severity**: Critical
- **Symptoms**: A dataset was removed or evicted from VectorStore.
- **Mitigation**:
  - Verify whether drop was intentional via admin API or triggered by storage eviction policies.
