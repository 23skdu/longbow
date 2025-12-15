# Persistence Architecture

Longbow uses a hybrid persistence model to ensure data durability and efficient storage.

## Write-Ahead Log (WAL)

*   **Format**: Row-oriented, append-only log.
*   **Purpose**: Provides immediate durability for incoming writes (Hot Path).
*   **Mechanism**: Every  operation appends the record to  before acknowledging success. This ensures crash consistency.
*   **Replay**: On startup, the WAL is replayed to restore the in-memory state.

## Snapshots

*   **Format**: Apache Parquet ().
*   **Purpose**: Efficient long-term storage and faster startup (Cold Storage).
*   **Mechanism**: 
    *   A background ticker triggers snapshots at configured intervals.
    *   In-memory Arrow records are serialized into Parquet files (one per dataset).
    *   Parquet offers better compression and is a standard format for analytics.
    *   After a successful snapshot, the WAL is truncated to free up space.

## Hybrid Workflow

1.  **Write**: Client sends data -> Appended to WAL -> Added to Memory.
2.  **Snapshot**: Timer fires -> Memory dumped to Parquet -> WAL truncated.
3.  **Recovery**: Load Parquet Snapshots -> Replay remaining WAL -> System Ready.

## Configuration

*   **Data Path**: Directory where WAL and snapshots are stored.
*   **Snapshot Interval**: Frequency of automatic snapshots.
