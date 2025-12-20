# Performance Testing Guide

This guide documents the Longbow performance testing suite located at
`scripts/perf_test.py`.

## Overview

The performance test suite provides comprehensive benchmarking for Longbow
vector store operations including:

- **Basic throughput** - DoPut/DoGet operations
- **Vector search** - HNSW similarity search
- **Hybrid search** - Dense vectors + sparse text search
- **Concurrent load** - Multi-client stress testing
- **Large vectors** - OpenAI/Anthropic embedding dimensions
- **S3 snapshots** - Snapshot backend performance
- **Memory pressure** - Behavior under memory limits

## Prerequisites

```bash
# Install dependencies
pip install pyarrow numpy pandas dask boto3

# Start Longbow server
./longbow --data-port 3000 --meta-port 3001
```

## Quick Start

```bash
# Basic test with defaults
python scripts/perf_test.py

# Full benchmark suite
python scripts/perf_test.py --all --rows 50000 --dim 768 --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001

# Export results to JSON
python scripts/perf_test.py --all --json results.json
```

## Benchmarks

### 1. Basic Throughput (DoPut/DoGet)

Measures raw data ingestion and retrieval throughput.

**Metrics:**

- Throughput (MB/s)
- Duration (seconds)
- Bytes processed

**Usage:**

```bash
python scripts/perf_test.py --rows 100000 --dim 128
```

**Code:**

```python
def benchmark_put(client, table, name):
    descriptor = flight.FlightDescriptor.for_path(name)
    start_time = time.time()
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()
    duration = time.time() - start_time
    mb = table.nbytes / 1024 / 1024
    throughput = mb / duration
    return BenchmarkResult(
        name="DoPut",
        duration_seconds=duration,
        throughput=throughput,
        throughput_unit="MB/s",
    )
```

### 2. Vector Similarity Search (HNSW)

Benchmarks HNSW index search performance with configurable k and query count.

**Metrics:**

- Queries per second (QPS)
- Latency percentiles (p50, p95, p99)
- Error rate

**Usage:**

```bash
python scripts/perf_test.py --search --search-k 10 --query-count 1000
```

**Code:**

```python
def benchmark_vector_search(client, name, query_vectors, k):
    latencies = []
    for qvec in query_vectors:
        query = {
            "name": name,
            "action": "search",
            "vector": qvec.tolist(),
            "k": k,
        }
        ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
        start = time.time()
        reader = client.do_get(ticket)
        results = reader.read_all()
        latencies.append((time.time() - start) * 1000)
    return BenchmarkResult(
        name="VectorSearch",
        throughput=len(query_vectors) / sum(latencies) * 1000,
        throughput_unit="queries/s",
        latencies_ms=latencies,
    )
```

### 3. Hybrid Search (Dense + Sparse)

Benchmarks combined vector similarity and text search using RRF fusion.

**Metrics:**

- Queries per second (QPS)
- Latency percentiles (p50, p95, p99)
- Error rate

**Usage:**

```bash
python scripts/perf_test.py --hybrid --search-k 10 --query-count 500
```

**Code:**

```python
def benchmark_hybrid_search(client, name, query_vectors, k, text_queries):
    latencies = []
    for qvec, text_query in zip(query_vectors, text_queries):
        query = {
            "name": name,
            "action": "hybrid_search",
            "vector": qvec.tolist(),
            "text_query": text_query,
            "k": k,
            "alpha": 0.5,  # balance dense/sparse
        }
        ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
        start = time.time()
        reader = client.do_get(ticket)
        results = reader.read_all()
        latencies.append((time.time() - start) * 1000)
    return BenchmarkResult(
        name="HybridSearch",
        throughput=qps,
        throughput_unit="queries/s",
        latencies_ms=latencies,
    )
```

### 4. Concurrent Load Testing

Stress tests with multiple parallel clients performing mixed operations.

**Metrics:**

- Operations per second
- Latency percentiles under load
- Error rate

**Usage:**

```bash
# Mixed read/write workload
python scripts/perf_test.py --concurrent 16 --duration 60

# Write-only workload
python scripts/perf_test.py --concurrent 16 --duration 60 --operation put

# Read-only workload
python scripts/perf_test.py --concurrent 16 --duration 60 --operation get
```

**Code:**

```python
def benchmark_concurrent_load(data_loc, name, dim, num_workers, duration_seconds):
    stop_event = threading.Event()
    all_latencies = []
    total_ops = 0

    def worker(worker_id):
        client = flight.FlightClient(data_loc)
        while not stop_event.is_set():
            # Perform mixed put/get operations
            start = time.time()
            # ... operation ...
            latencies.append((time.time() - start) * 1000)

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker, i) for i in range(num_workers)]
        time.sleep(duration_seconds)
        stop_event.set()

    return BenchmarkResult(
        name="Concurrent_mixed",
        throughput=total_ops / duration_seconds,
        throughput_unit="ops/s",
    )
```

### 5. Large Dimension Vectors

Tests performance with production embedding sizes.

**Common Dimensions:**

| Model | Dimension |
|-------|----------:|
| OpenAI text-embedding-3-small | 1536 |
| OpenAI text-embedding-3-large | 3072 |
| Anthropic | 1024 |
| Cohere | 1024 |
| Custom | 128-4096 |

**Usage:**

```bash
# OpenAI embeddings
python scripts/perf_test.py --rows 50000 --dim 1536

# Anthropic embeddings
python scripts/perf_test.py --rows 50000 --dim 3072
```

### 6. S3 Snapshot Benchmark

Measures snapshot write performance to S3-compatible storage.

**Metrics:**

- Throughput (MB/s)
- Duration (seconds)
- Bytes written

**Usage:**

```bash
# With MinIO
python scripts/perf_test.py --s3 --s3-endpoint localhost:9000 --s3-bucket longbow-test

# With AWS S3
python scripts/perf_test.py --s3 --s3-endpoint s3.amazonaws.com --s3-bucket my-bucket
```

**Code:**

```python
def benchmark_s3_snapshot(client, name, s3_endpoint, s3_bucket):
    snapshot_request = json.dumps({
        "action": "snapshot",
        "collection": name,
        "backend": "s3",
        "endpoint": s3_endpoint,
        "bucket": s3_bucket,
    }).encode("utf-8")

    start = time.time()
    action = flight.Action("snapshot", snapshot_request)
    results = list(client.do_action(action))
    duration = time.time() - start

    return BenchmarkResult(
        name="S3Snapshot",
        throughput=bytes_written / duration / 1024 / 1024,
        throughput_unit="MB/s",
    )
```

### 7. Memory Pressure Testing

Tests eviction behavior when approaching memory limits.

**Metrics:**

- Rows ingested before pressure
- Eviction count
- Throughput degradation

**Usage:**

```bash
python scripts/perf_test.py --memory-pressure --memory-limit 512
```

**Code:**

```python
def benchmark_memory_pressure(data_loc, memory_limit_mb, dim):
    # Calculate rows to exceed limit
    bytes_per_row = dim * 4 + 8 + 8
    target_bytes = memory_limit_mb * 1024 * 1024 * 2  # 2x limit
    target_rows = target_bytes // bytes_per_row

    while total_rows < target_rows:
        batch = generate_vectors(batch_rows, dim)
        try:
            # Insert batch
            writer.write_table(batch)
        except flight.FlightError as e:
            if "memory" in str(e).lower():
                evictions += 1

    return BenchmarkResult(
        name="MemoryPressure",
        throughput=rows_per_sec,
        throughput_unit="rows/s",
    )
```

## Command Reference

### Connection Options

| Flag | Default | Description |
|------|---------|-------------|
| `--data-uri` | grpc://0.0.0.0:3000 | Data Server URI |
| `--meta-uri` | grpc://0.0.0.0:3001 | Meta Server URI |

### Data Generation Options

| Flag | Default | Description |
|------|---------|-------------|
| `--rows` | 10000 | Number of vectors to generate |
| `--dim` | 128 | Vector dimension |
| `--name` | perf_test | Dataset name |

### Search Options

| Flag | Default | Description |
|------|---------|-------------|
| `--search` | false | Enable vector search benchmark |
| `--hybrid` | false | Enable hybrid search benchmark |
| `--search-k` | 10 | Top-k results to retrieve |
| `--query-count` | 1000 | Number of search queries |
| `--text-field` | meta | Text field for sparse search |

### Concurrent Load Options

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrent` | 0 | Number of parallel clients |
| `--duration` | 60 | Test duration in seconds |
| `--operation` | mixed | Operation type: put, get, mixed |

### Memory Options

| Flag | Default | Description |
|------|---------|-------------|
| `--memory-pressure` | false | Enable memory pressure test |
| `--memory-limit` | 512 | Memory limit in MB |

### S3 Options

| Flag | Default | Description |
|------|---------|-------------|
| `--s3` | false | Enable S3 snapshot benchmark |
| `--s3-endpoint` | localhost:9000 | S3 endpoint |
| `--s3-bucket` | longbow-snapshots | S3 bucket name |

### Output Options

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | none | Export results to JSON file |
| `--all` | false | Run all benchmarks |

## Output Format

### Console Output

```text
======================================================================
BENCHMARK SUMMARY
======================================================================
Benchmark            Duration    Throughput      p50      p95      p99
----------------------------------------------------------------------
DoPut                   1.23s     156.78 MB/s     N/A      N/A      N/A
DoGet                   0.89s     215.43 MB/s     N/A      N/A      N/A
VectorSearch            5.67s    1234.56 queries/s   2.1ms   4.5ms   8.2ms
HybridSearch            8.12s     567.89 queries/s   3.4ms   7.8ms  12.1ms
Concurrent_mixed       60.00s    4567.89 ops/s   1.2ms   3.4ms   6.7ms
======================================================================
```

### JSON Output

```json
[
  {
    "name": "DoPut",
    "duration_seconds": 1.23,
    "throughput": 156.78,
    "throughput_unit": "MB/s",
    "rows": 100000,
    "bytes_processed": 192000000,
    "p50_ms": 0,
    "p95_ms": 0,
    "p99_ms": 0,
    "errors": 0
  }
]
```

## Best Practices

### Baseline Testing

1. Run basic throughput first to establish baseline
2. Use consistent hardware for comparisons
3. Run multiple iterations and average results

### Search Testing

1. Ensure data is indexed before running search benchmarks
2. Use representative query distributions
3. Test with production embedding dimensions

### Load Testing

1. Start with few workers and increase gradually
2. Monitor server resource utilization
3. Test both read-heavy and write-heavy workloads

### Memory Testing

1. Set realistic memory limits based on deployment
2. Monitor for OOM conditions
3. Test recovery after evictions

## Troubleshooting

### Connection Errors

```bash
# Verify server is running
curl -v telnet://localhost:3000

# Check server logs
docker logs longbow
```

### Slow Performance

1. Check vector dimensions match expected
2. Verify SIMD optimizations are active
3. Monitor memory pressure

### Search Errors

1. Ensure data is inserted before searching
2. Verify index is built (check metrics)
3. Check query vector dimensions match data
