# Longbow Performance & Test Scripts

This directory contains utility scripts for testing, benchmarking, and validating the Longbow Vector Store.

## Key Scripts

### 1. `ops_test.py`

The primary operational test CLI for interacting with the Longbow server. It supports both Data Plane and Meta Plane operations.

**Supported Commands:**

- **Data Operations**:
  - `put`: Upload synthetic vector data.
  - `get`: Download datasets (supports filtering).
  - `exchange`: Test bidirectional Arrow Flight DoExchange.
- **Vector Operations**:
  - `search`: Perform vector similarity search (HNSW) or Hybrid search.
  - `delete`: Delete vectors by ID.
  - `list`: List all available datasets.
  - `info`: Get schema and size information for a dataset.
- **GraphRAG Operations**:
  - `graph-stats`: Retrieve graph statistics (edge count, community count).
  - `add-edge`: Add edges to the knowledge graph.
  - `traverse`: Perform graph traversal from a start node.
- **Cluster Operations**:
  - `status`: Check cluster membership and health.
  - `snapshot`: Trigger a manual snapshot (S3 or local).
  - `namespaces`: Manage namespaces (create, list, delete).
  - `validate`: Run a full end-to-end validation suite.

**Usage Example:**

```bash
# Upload data
python3 ops_test.py put --dataset test_ds --rows 1000

# Vector Search
python3 ops_test.py search --dataset test_ds --k 10

# Graph Statistics
python3 ops_test.py graph-stats --dataset test_ds
```

### 2. `perf_test.py`

Comprehensive benchmarking suite for performance characterization.

**Features:**

- **Throughput Testing**: Benchmark DoPut/DoGet performance (MB/s, rows/s).
- **Latency Testing**: Measure p50, p95, p99 latencies for searches.
- **Graph Benchmarks**: Benchmark graph traversal performance.
- **Stress Testing**: Concurrent load generation, memory pressure testing.
- **Hybrid Search**: Benchmark combined vector + text search.

**Usage Example:**

```bash
# General Benchmark
python3 perf_test.py --all

# Graph Traversal Benchmark
python3 perf_test.py --graph --graph-nodes 100 --graph-hops 2
```

### 3. `test_scripts.py`

Unit and integration tests for `ops_test.py` and `perf_test.py` to ensure CLI commands and logic work correctly.

## Other Utility Scripts

| Script | Purpose |
|--------|---------|
| `start_local_cluster.sh` | Starts a local single-node cluster (Data port 3000, Meta port 3001). |
| `start_test_cluster.sh` | Starts a multi-node local cluster for distributed testing. |
| `run_cluster_bench.sh` | Orchestrates benchmarks against a running cluster. |
| `soak_test.py` | Long-running test script to detect memory leaks and stability issues. |
| `verify_global_search.py` | Verifies distributed search results across multiple nodes. |
| `metrics_validation.py` | Validates Prometheus metrics exposed by the server. |
| `distributed_test_k8s.sh` | Helper for running distributed tests in a Kubernetes environment. |

## Requirements

```bash
pip install -r requirements.txt
# OR manually:
pip install pyarrow pandas numpy polars boto3 dask[dataframe]
```
