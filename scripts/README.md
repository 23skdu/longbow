# Longbow Scripts Directory

This directory contains utility tools for testing, benchmarking, and deploying Longbow.

## Main Tools

### unified_benchmark.py

The primary benchmark tool for Longbow performance testing. Runs ingest and search benchmarks across multiple dimensions, data types, and counts. Supports CPU, Metal (macOS), and CUDA (Linux) modes.

**Modes**: cpu, metal, cuda, onnx, recommend, deletion, graphrag, exchange, cluster, temporal

**Usage**:
  ```bash
  python3 scripts/unified_benchmark.py --mode cpu
  python3 scripts/unified_benchmark.py --mode metal
  python3 scripts/unified_benchmark.py --dims 128,384 --counts 1000,5000,10000 --dtypes float32,int8,complex64
  ```

**Arguments**:
- `--mode`: Backend mode (cpu, metal, cuda, onnx, recommend, deletion, graphrag, exchange, cluster, temporal)
- `--dims`: Comma-separated dimensions (default: 128,384)
- `--counts`: Comma-separated batch sizes (default: 500,1000,3000,7000,15000,25000,50000)
- `--dtypes`: Comma-separated data types (default: all 14 types)
- `--memory`: Maximum memory in GB (default: 18)
- `--queries`: Number of queries per test (default: 1000)
- `--duration`: Duration in seconds for Go benchmarks (default: 60)

Supports exhaustive matrix testing across dimensions, data types, and batch sizes. Generates JSON results and Markdown reports automatically.

### seq_benchmark.py

Sequential benchmark runner that runs tests one configuration at a time. Useful for debugging and when you need precise control over each test run.

**Usage**:
  ```bash
  python3 scripts/seq_benchmark.py --mode cpu --dim 128 --dtype float32 --count 10000
  ```

### graph_functional_test.py

Functional test runner for GraphRAG features. Tests graph spreading activation operations and validates graph traversal correctness.

**Usage**:
  ```bash
  python3 scripts/graph_functional_test.py --uri 127.0.0.1:3000
  ```

## Shell Scripts

### run_full_bench.sh

Shell wrapper script for running full benchmark suites. Executes unified_benchmark.py with predefined test matrices.

**Usage**:
  ```bash
  ./scripts/run_full_bench.sh
  ```

### run_all_local.sh

Runs all benchmark modes locally (CPU, Metal if on macOS). Useful for quick local validation.

**Usage**:
  ```bash
  ./scripts/run_all_local.sh
  ```

### run_user_request_bench.sh

Runs benchmark suites based on user request parameters. Accepts dimension, count, and dtype arguments.

**Usage**:
  ```bash
  ./scripts/run_user_request_bench.sh --dims 128,384 --counts 1000,5000 --dtypes float32,int8
  ```

### cluster_tool.sh

Kubernetes environment management and resilience testing utility.

**Commands**:
- `up`: Provision a local Kind cluster
- `deploy`: Build and load Docker images into the cluster
- `test-dist`: Run distributed testing scenarios across multiple namespaces
- `chaos-partition`: Simulate a network partition (isolates a node)
- `chaos-heal`: Resolve active partitions
- `down`: Tear down the cluster

**Usage**:
  ```bash
  ./scripts/cluster_tool.sh up
  ./scripts/cluster_tool.sh deploy
  ./scripts/cluster_tool.sh test-dist
  ./scripts/cluster_tool.sh down
  ```

## Additional Tools

### bench-tool

Go benchmark binary located at `bin/bench-tool`. Performs ingest and search benchmarking against a running Longbow server.

**Features**:
- Connects to Longbow via gRPC
- Supports all data types: float32, float16, int8, int64, complex64, complex128, turboquant
- Outputs JSON results for programmatic analysis

**Usage**:
  ```bash
  ./bin/bench-tool --uri=127.0.0.1:3000 --dim=128 --dtype=float32 --scale=10000
  ```

## Requirements

Install Python dependencies before running any Python tools:

```bash
pip install -r scripts/requirements.txt
```

## Notes

- Most scripts expect a running Longbow instance or cluster
- Default ports: 3000 (data), 3001 (meta), 9090 (metrics)
- Generated logs and profiles are gitignored
- Results are saved to `data/perf_logs/perf_matrix_*.json`