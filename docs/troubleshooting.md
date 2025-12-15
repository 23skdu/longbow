# Troubleshooting Guide

This guide maps common symptoms to potential causes and solutions using Longbow's metrics and logs.

## Common Issues

### 1. Slow Write Performance

**Symptom**: `DoPut` operations are taking longer than expected.

**Check Metrics**:
* `longbow_wal_writes_total`: Is the rate consistent?
* `longbow_wal_bytes_written_total`: Are you writing unusually large batches?

**Potential Causes**:
* **Slow Disk**: The WAL requires high IOPS. Ensure `LONGBOW_DATA_PATH` is on an SSD.
* **Large Batches**: Extremely large Arrow batches can cause GC pauses. Try reducing batch size.

### 2. High Memory Usage

**Symptom**: Pod is getting OOMKilled or memory usage is climbing indefinitely.

**Check Metrics**:
* `longbow_vector_index_size`: Is the index growing as expected?
* `longbow_memory_fragmentation_ratio`: Is Go runtime retaining memory?

**Potential Causes**:
* **Snapshot Lag**: If snapshots are failing, the WAL grows, and memory isn't freed. Check `longbow_snapshot_operations_total{status="error"}`.
* **Configuration**: Ensure `LONGBOW_MAX_MEMORY` is set to a value lower than your container's hard limit.

### 3. Slow Startup

**Symptom**: Longbow takes a long time to become ready after a restart.

**Check Metrics**:
* `longbow_wal_replay_duration_seconds`: High values indicate a large WAL.

**Solution**:
* Decrease `LONGBOW_SNAPSHOT_INTERVAL`. A shorter interval means a smaller WAL to replay on startup, as older data is already in Parquet.
