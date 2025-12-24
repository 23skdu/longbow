# Troubleshooting Guide

This guide maps common symptoms to potential causes and solutions using
Longbow's metrics and logs.

## Common Issues

### 1. Slow Write Performance

**Symptom**: `DoPut` operations are taking longer than expected.

**Check Metrics**:

* `longbow_wal_writes_total`: Is the rate consistent?
* `longbow_wal_bytes_written_total`: Are you writing unusually large batches?

**Potential Causes**:

* **Slow Disk**: The WAL requires high IOPS. Ensure `LONGBOW_DATA_PATH` is on an
  SSD.
* **Large Batches**: Extremely large Arrow batches can cause GC pauses. Try
  reducing batch size.

### 2. High Memory Usage

**Symptom**: Pod is getting OOMKilled or memory usage is climbing indefinitely.

**Check Metrics**:

* `longbow_vector_index_size`: Is the index growing as expected?
* `longbow_memory_fragmentation_ratio`: Is Go runtime retaining memory?

**Potential Causes**:

* **Snapshot Lag**: If snapshots are failing, the WAL grows, and memory isn't
  freed. Check `longbow_snapshot_operations_total{status="error"}`.
* **Configuration**: Ensure `LONGBOW_MAX_MEMORY` is set to a value lower than
  your container's hard limit.

### 3. Slow Startup

**Symptom**: Longbow takes a long time to become ready after a restart.

**Check Metrics**:

* `longbow_wal_replay_duration_seconds`: High values indicate a large WAL.

**Solution**:

* Decrease `LONGBOW_SNAPSHOT_INTERVAL`. A shorter interval means a smaller WAL
  to replay on startup, as older data is already in Parquet.

### 4. Permission Denied on /data

**Symptom**: Pod crashes with `open /data/wal.log: permission denied`.

**Cause**: The application runs as a non-root user (UID 1000) while `/data` is owned by root or the filesystem is read-only.

**Solution**:

* Ensure `persistence.wal.enabled` is `true` in Helm values to mount a PersistentVolume.
* Verify `podSecurityContext.fsGroup` is set to `2000` (or similar) to ensure the volume is writable by the app user.

### 5. Config Parsing Errors

**Symptom**: `panic: Failed to process config: converting '6.7108864e+07' to type int`.

**Cause**: Helm passes large numeric values as floating-point scientific notation if not explicitly quoted.

**Solution**:

* Quote all large integer values in `values.yaml` (e.g., `maxRecvMsgSize: "67108864"`).

### 6. Replication Lag

**Symptom**: `longbow_replication_lag_seconds` is high (> 30s) on follower nodes.

**Cause**: Network variance, slow disk I/O on follower, or high write throughput overwhelming replication stream.

**Solution**:

* Check follower disk IOPS and CPU.
* Ensure network connectivity between Leader and Follower is stable (`longbow_gossip_pings_total{direction="failed"}`).
* If persisting, consider scaling out with more shards to distribute write load.

### 7. GPU Initialization Failure

**Symptom**: Log shows `WARN GPU initialization failed, using CPU-only`.

**Cause**:

* **CUDA/Metal**: Missing drivers or unsupported hardware.
* **Memory**: Insufficient GPU memory (OOM).
* **Permissions**: Access to GPU device denied.

**Solution**:

* Verify NVIDIA drivers/CUDA toolkit (Linux) or macOS version (Apple Silicon).
* Check `nvidia-smi` or `powermetrics` (macOS).
* Ensure `GPU_ENABLED=true` is set.

### 8. S3 Backup Failures

**Symptom**: `longbow_s3_operations_total{status="error"}` is increasing.

**Cause**: AWS Credentials expiry, bucket policy denial, or network timeouts.

**Solution**:

* Verify `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
* Check IAM permissions for `s3:PutObject` and `s3:GetObject`.
* Inspect logs for specific S3 error codes (e.g., `403 Forbidden`, `503 Slow Down`).
