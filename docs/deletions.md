# Deletions and Tombstones in Longbow

Longbow implements a high-performance, non-blocking deletion model inspired by LSM-trees. Instead of immediately re-writing large segments of the index or data files, it utilizes a "Soft-Delete" strategy that ensures consistent search performance with minimal mutation overhead.

## 1. The Tombstone Mechanism

When a record is deleted or updated in Longbow, it is not immediately removed from memory or disk. Instead:

1.  **Tombstone Marking**: The system identifies the physical location (Batch Index + Row Offset) of the record.
2.  **Bitset Update**: A bit is set in a **Tombstone Bitset** associated with the specific Arrow RecordBatch containing the data.
3.  **Primary Index Removal**: The record ID is removed from the `PrimaryIndex` (mapping IDs to locations), preventing new queries from finding it via ID.

### Why Tombstones?
*   **Speed**: Setting a bit in a bitset is a sub-microsecond operation.
*   **Concurrency**: Multiple threads can mark tombstones simultaneously without complex locking of the underlying data arrays.
*   **Search Masking**: Longbow's SIMD-accelerated distance kernels use the tombstone bitset as a mask. Deleted records are skipped at the lowest level of the compute pipeline, incurring zero performance penalty during search traversal.

---

## 2. Data Lifecycle

### Updates (Upserts)
Longbow treats updates as an atomic **Delete + Insert** operation:
1.  The existing version of the ID is tombstoned.
2.  The new version is appended to the current active RecordBatch.
3.  The `PrimaryIndex` is updated to point to the new location.
4.  The WAL (Write-Ahead Log) records both actions to ensure consistency after a crash.

### Compaction (Garbage Collection)
To prevent "dead space" from accumulating, Longbow's **Compaction Worker** monitors fragmentation.

*   **Fragmentation Ratio**: Each dataset tracks the ratio of tombstoned rows to total rows.
*   **Threshold**: When a batch exceeds a threshold (typically 20%), it is marked for compaction.
*   **Squashing**: During compaction, the worker creates a new, dense RecordBatch by copying only the active (non-tombstoned) rows.
*   **Atomic Swap**: The old batch is released, and the new batch is integrated into the dataset. The `SlabArena` then reclaims the memory from the old batch.

---

## 3. Namespace Interaction

Namespaces provide a bulk lifecycle management layer:

*   **Bulk Deletion**: Deleting a namespace recursively drops all contained datasets.
*   **Resource Reclamation**: When a namespace is deleted, its memory is immediately returned to the system-wide pool, and its persistent snapshots/WAL logs are purged from the filesystem.
*   **Isolation**: Tombstones are scoped to a dataset within a namespace. Deleting a record in one namespace has no effect on identical IDs in another namespace.

## 4. Operational Best Practices

*   **Monitor Fragmentation**: Use the `longbow_dataset_fragmentation_ratio` metric to monitor how much space is being consumed by tombstones.
*   **Tune Compaction**: If your workload involves heavy updates, consider lowering the `LONGBOW_COMPACTION_THRESHOLD` to reclaim memory more frequently.
*   **Bulk Cleanup**: For temporary data (e.g., a per-session cache), prefer using a dedicated **Namespace** and deleting the entire namespace when the session ends, rather than deleting individual records.
