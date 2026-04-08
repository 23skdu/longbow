# Temporal Query Capabilities

**Last Updated**: 2026-04-07

Longbow supports temporal queries that allow searching and managing vectors based on time-based criteria. This is essential for AI/ML workloads involving time-series data, version history, and time-constrained searches.

---

## Overview

Temporal query capabilities enable:

- **Time-constrained vector search** - Search vectors as of a specific timestamp or within a time range
- **Version history** - Track multiple versions of vectors over time with automatic pruning
- **TTL expiration** - Automatic cleanup of old vectors based on time-to-live policies
- **Time-bucket aggregation** - Analytics over time windows (count, min, max, mean)

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ENABLED` | false | Enable temporal index |
| `TEMPORAL_VERSION_HISTORY` | false | Enable version history tracking |
| `TEMPORAL_MAX_VERSIONS` | 10 | Max versions per vector |
| `TEMPORAL_RETENTION_PERIOD` | 168h (7 days) | Version retention period |
| `TEMPORAL_TTL_ENABLED` | false | Enable TTL expiration |
| `TEMPORAL_DEFAULT_TTL` | 720h (30 days) | Default TTL for vectors |
| `TEMPORAL_CLEANUP_INTERVAL` | 1h | TTL cleanup interval |
| `TEMPORAL_AGGREGATION_ENABLED` | false | Enable temporal aggregation |
| `TEMPORAL_MAX_BUCKETS` | 1000 | Max aggregation buckets |

### Example

```bash
export TEMPORAL_ENABLED=true
export TEMPORAL_VERSION_HISTORY=true
export TEMPORAL_MAX_VERSIONS=10
export TEMPORAL_TTL_ENABLED=true
export TEMPORAL_DEFAULT_TTL=720h
```

---

## API Reference

### Temporal Search Types

Longbow supports four temporal search types:

#### 1. As-Of Search (`as_of`)

Search for vectors as they existed at a specific point in time.

```json
{
  "search_type": "as_of",
  "timestamp": 1704067200000000000,
  "k": 10
}
```

#### 2. Range Search (`range`)

Search for vectors within a time range.

```json
{
  "search_type": "range",
  "start_time": 1703980800000000000,
  "end_time": 1704067200000000000,
  "k": 10
}
```

#### 3. Sliding Window (`sliding_window`)

Search the N most recent vectors.

```json
{
  "search_type": "sliding_window",
  "window_size": 100,
  "k": 10
}
```

#### 4. Sliding Window by Time (`sliding_window_time`)

Search vectors within a duration looking back from now.

```json
{
  "search_type": "sliding_window_time",
  "duration": "1h",
  "k": 10
}
```

---

## Python SDK Usage

### Initialize Client

```python
from longbow import LongbowClient

client = LongbowClient("grpc://localhost:3000")
client.connect()
```

### Temporal Search

```python
# As-of search
results = client.temporal_search(
    search_type="as_of",
    timestamp=1704067200000000000,
    k=10
)

# Range search
results = client.temporal_search(
    search_type="range",
    start_time=1703980800000000000,
    end_time=1704067200000000000,
    k=10
)

# Sliding window
results = client.temporal_search(
    search_type="sliding_window",
    window_size=100,
    k=10
)

# Sliding window by time
results = client.temporal_search(
    search_type="sliding_window_time",
    duration="1h",
    k=10
)
```

### Version History

```python
# Get version history for a vector
history = client.temporal_version_history(vector_id=12345)
print(history)
```

### Temporal Aggregation

```python
# Count aggregation over time buckets
result = client.temporal_aggregation(
    aggregation_type="count",
    start_time=1703980800000000000,
    end_time=1704067200000000000,
    interval=3600000000000  # 1 hour in nanoseconds
)
print(result)
```

---

## Architecture

### Components

| Component | Description |
|-----------|-------------|
| `TemporalIndex` | In-memory index with temporal tree for O(log n) time lookups |
| `TemporalTree` | Sorted timestamp tree for efficient range queries |
| `VersionHistory` | Multi-version storage with max versions and retention |
| `TTLPolicy` | Background goroutine for automatic expiration |
| `TemporalAggregator` | Time-bucket aggregation engine |

### Data Structures

```go
type TemporalVector struct {
    ID        uint64
    Vector    []float32
    Timestamp int64  // Unix nanoseconds
    Metadata  map[string]interface{}
    Tombstone bool
}

type TemporalIndex struct {
    dimension    int
    vectors      map[uint64]*TemporalVector
    temporalTree *TemporalTree
    byTimestamp  map[int64][]uint64
}
```

### Time Complexity

| Operation | Complexity |
|-----------|------------|
| Insert | O(log n) |
| Search As-Of | O(log n + k) |
| Range Search | O(log n + r) |
| Sliding Window | O(k) |
| Version History | O(1) |

---

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `temporal_search_total` | Counter | Total temporal searches |
| `temporal_search_duration_seconds` | Histogram | Temporal search latency |
| `temporal_version_history_size` | Gauge | Version history entries |
| `temporal_ttl_expired_total` | Counter | Vectors expired by TTL |
| `temporal_aggregation_duration_seconds` | Histogram | Aggregation latency |
| `temporal_index_size` | Gauge | Total temporal vectors |

---

## Use Cases

### 1. Time-Series Embeddings

Track embeddings over time for monitoring model drift:

```python
# Search recent vectors
results = client.temporal_search(
    search_type="sliding_window_time",
    duration="24h",
    k=10
)
```

### 2. Audit Trail

Maintain version history for compliance:

```python
# Get all versions of a vector
history = client.temporal_version_history(vector_id=12345)
for version in history:
    print(f"Version {version['version']} at {version['timestamp']}")
```

### 3. Real-Time Analytics

Aggregate vectors over time windows:

```python
# Count vectors per hour
result = client.temporal_aggregation(
    aggregation_type="count",
    start_time=start_time,
    end_time=end_time,
    interval=3600000000000
)
```

### 4. Data Lifecycle Management

Auto-expire old vectors to manage storage:

```bash
export TEMPORAL_TTL_ENABLED=true
export TEMPORAL_DEFAULT_TTL=720h  # 30 days
```

---

## Benchmarking

Run temporal benchmarks:

```bash
python scripts/unified_benchmark.py --temporal
```

See [Performance](performance.md) for detailed benchmark results.

---

## Related Documentation

- [Vector Search Architecture](vectorsearch.md)
- [Configuration](configuration.md)
- [Metrics](metrics.md)
