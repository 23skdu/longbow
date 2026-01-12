# Longbow Scripts Directory

This directory contains utility scripts for testing, benchmarking, and deploying Longbow.

## Testing Scripts

### `test_filters_comprehensive.py`

Comprehensive filter testing suite that validates:

- Schema validation
- Filter operators (eq, gt, lt, neq) with various case formats (eq, Eq, EQ)
- Numeric and string filters
- Raw request flow debugging

**Usage:**

```bash
python scripts/test_filters_comprehensive.py
```

### `test_ingestion.py`

Tests basic data ingestion functionality.

### `test_scripts.py`

General test utilities and helpers.

### `ops_test.py`

Operational testing for various Longbow operations.

## Benchmarking Scripts

### `benchmark_comprehensive.py`

Main comprehensive benchmark suite testing DoPut, DoGet, and VectorSearch across multiple scales and dimensions.

**Usage:**

```bash
python scripts/benchmark_comprehensive.py
```

### `perf_test.py`

Detailed performance testing tool with extensive configuration options.

**Usage:**

```bash
python scripts/perf_test.py --rows 10000 --dim 384 --search --hybrid
```

### `profile_ingestion.py`

Profiles ingestion performance and collects pprof data.

### `run_fresh_benchmarks.py`

Runs fresh benchmark suite with clean cluster state.

## Deployment Scripts

### Cluster Management

#### `start_local_cluster.sh`

Starts a 3-node local cluster with 6GB RAM per node.

**Usage:**

```bash
./scripts/start_local_cluster.sh
```

#### `start_3node_6gb.sh`

Alternative 3-node cluster startup script.

#### `start_one_node.sh`

Starts a single Longbow node for development/testing.

### Soak Testing

#### `soak_test.sh`

Runs soak tests on the cluster.

#### `soak_test.py`

Python-based soak test implementation.

### Kubernetes

#### `setup_kind_test.sh`

Sets up KIND (Kubernetes in Docker) for testing.

#### `setup_multi_namespace_test.sh`

Sets up multi-namespace Kubernetes test environment.

#### `distributed_test_k8s.sh`

Runs distributed tests on Kubernetes.

## Example Scripts

### `demo_graphrag_embeddings.py`

Demonstrates GraphRAG embedding usage with Longbow.

### `example_distance_metrics.py`

Shows different distance metric implementations.

### `lorem_vector_test.py`

Large-scale test using Lorem Ipsum text data.

## Utility Scripts

### `metrics_validation.py`

Validates Prometheus metrics output.

### `verify_global_search.py`

Verifies global search functionality across cluster.

### `verify_pipelining.py`

Tests request pipelining performance.

### `update_dashboard.py` / `enhance_dashboard.py`

Dashboard management utilities.

## Shell Scripts

### `benchmark_suite.sh`

Orchestrates full benchmark suite execution.

### `manual_benchmark.sh`

Manual benchmark runner for custom configurations.

### `run_cluster_bench.sh`

Runs benchmarks on cluster deployment.

### `run_comparison_bench.sh`

Compares performance across different configurations.

### `run_extended_perf_tests.sh`

Extended performance test suite.

### `run_soak.sh`

Soak test orchestration.

### `validate_cluster_soak.sh`

Validates cluster health during soak tests.

### `validate_metrics.sh`

Validates metrics collection and reporting.

### `verify_soak_fix.sh`

Verifies soak test fixes.

### `stress_test.sh`

Stress testing script.

### `partition_test.sh`

Tests network partition handling.

## Requirements

Install Python dependencies:

```bash
pip install -r scripts/requirements.txt
```

## Notes

- Generated files (*.log, *.prof, results_*.json) are gitignored
- Most scripts expect a running Longbow cluster
- Default cluster ports: 3000 (data), 3001 (meta), 9090 (metrics)
