# Admin API

Longbow provides management capabilities via the **Meta Server** (`DoAction`).

## Operations

Operations are performed by sending a `DoAction` request with a JSON body (if applicable).

### `force_snapshot`

Triggers an immediate snapshot of all in-memory data to the configured persistence backend (Local or S3).

- **Type**: `force_snapshot`
- **Body**: (empty)
- **Response**: `{"status": "ok", "message": "snapshot created"}` or error.

### `drop_dataset`

Removes a dataset from memory. Does **not** delete snapshots from S3/Disk immediately 
(snapshots are immutable, but future replays won't load it if the WAL drop entry is processed - *Note: Drop persistence depends on implementation specifics*).

- **Type**: `drop_dataset`
- **Body**: `[dataset_name]` (string)
- **Response**: `{"status": "ok", "message": "dataset dropped"}`

### `get_stats`

Retrieves internal server statistics.

- **Type**: `get_stats`
- **Body**: (empty)
- **Response**:
  ```json
  {
    "datasets": 12,
    "current_memory": 104857600,
    "max_memory": 1073741824
  }
  ```

## Backpressure Monitoring

The Data Server (`DoPut`) monitors the WAL queue depth. If the queue exceeds 80% capacity:

1. Server logs a warning.
2. `DoPut` response includes metadata: `{"status": "slow_down", "reason": "wal_pressure"}`.

Clients should monitor this metadata and implement exponential backoff.
