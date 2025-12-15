# Data Eviction Policies

Longbow implements two eviction policies to manage memory usage and ensure
system stability: **Time-To-Live (TTL)** and **Least Recently Used (LRU)**.

## Time-To-Live (TTL)

TTL eviction removes datasets that have not been accessed (read or written) for
a specified duration. This is useful for cleaning up stale data automatically.

### TTL Configuration

- **Environment Variable**: `LONGBOW_TTL`
- **Default**: `0s` (Disabled)
- **Format**: Go duration string (e.g., `1h`, `30m`, `24h`)

When enabled, a background ticker runs periodically (at 1/10th of the TTL
interval, minimum 1s) to check for and remove expired datasets.

## Least Recently Used (LRU)

LRU eviction is triggered when the memory usage exceeds the configured maximum
limit during a write operation (`DoPut`). Longbow will evict the least recently
accessed datasets until enough memory is freed to accommodate the new data.

### LRU Configuration

- **Environment Variable**: `LONGBOW_MAX_MEMORY`
- **Default**: `1073741824` (1GB)
- **Behavior**: If the limit is reached and eviction cannot free enough space
  (or if the store is empty but the record is too large), the write operation
  fails with a "resource exhausted" error.

## Metrics

Eviction events are tracked via Prometheus metrics:

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `longbow_evictions_total` | Counter | `reason` (ttl/lru) | Total number of evicted datasets. |

## Implementation Details

- **Tracking**: Every dataset is wrapped in a structure that tracks its
  `LastAccess` timestamp.
- **Updates**: The timestamp is updated on every `DoGet` (read) and `DoPut`
  (write) operation.
- **Concurrency**: Eviction operations are thread-safe and protected by the
  store's mutex.
