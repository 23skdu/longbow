# Longbow Scripts Directory

This directory contains utility tools for testing, benchmarking, and deploying Longbow.

## Main Tools

### unified_benchmark.py

The primary benchmark tool for Longbow correctness and performance testing. Runs comprehensive matrix tests across dimensions, data types, and counts. Supports CPU, Metal (macOS), and CUDA (Linux) modes.

**Modes**: cpu, metal, cuda, onnx, recommend, deletion, graphrag, exchange, cluster, temporal

**Testing Types**:
- **Correctness**: Validates search result accuracy across all modes
- **Search**: Throughput and latency measurements
- **Scale**: Large dataset stress testing

**Key Arguments**:
- `--mode`: Backend mode (cpu, metal, cuda, onnx, recommend, deletion, graphrag, exchange, cluster, temporal)
- `--dims`: Comma-separated dimensions (default: 128,384)
- `--counts`: Comma-separated batch sizes (default: 500,1000,3000,7000,15000,25000,50000)
- `--dtypes`: Comma-separated data types (default: all 14 types)
- `--memory`: Maximum memory in GB (default: 18)
- `--queries`: Number of queries per test (default: 1000)
- `--duration`: Duration in seconds for Go benchmarks (default: 60)
- `--test-type`: Test type (correctness, search, scale) - **NEW**

**Usage**:
  ```bash
  # Correctness validation
  python3 scripts/unified_benchmark.py --mode cpu --dims 128,384 --test-type correctness
  
  # Search throughput
  python3 scripts/unified_benchmark.py --mode cpu --test-type search
  
  # Scale stress test
  python3 scripts/unified_benchmark.py --mode metal --test-type scale --counts 50000,100000
  
  # Full matrix
  python3 scripts/unified_benchmark.py --mode cpu --dims 128,384 --counts 1000,5000 --dtypes float32,int8,complex64
  --search-modes dense,hybrid,sparse,filtered,byid
  ```

**Features**:
- Auto server start/stop (via get_cli_tool() method)
- SDK client integration (via get_sdk_client() method)
- Exhaustive matrix testing
- JSON results and Markdown reports

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

### bench_functional_test.sh

Functional test runner for bench-tool and longbow-cli. Starts a Longbow server, runs benchmark tests, and validates results.

**Features**:
- Starts/stops Longbow server automatically
- Tests vector benchmark modes (ingest, dense, hybrid, sparse, filtered, byid)
- Validates JSON output
- Cleans up after tests

**Usage**:
  ```bash
  ./scripts/bench_functional_test.sh
  ```

**Test Matrix**:
- Dimensions: 128, 384, 768
- Scales: 1000, 5000, 10000
- Search modes: dense, hybrid, sparse, filtered, byid

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

### test_cli.sh

Comprehensive verification script for the `longbow-cli`. Validates the full feature matrix (dimensions, datatypes) and specialized features like Geospatial and GraphRAG.

**Usage**:
  ```bash
  ./scripts/test_cli.sh
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

### bench-tool (bin/bench-tool)

Go benchmark binary for high-performance server testing. Located at `bin/bench-tool`.

**Modes**:
- `write`, `read`, `mixed`: Traditional I/O benchmarks
- `vec`: Vector benchmark mode (ingest + search)

**Vector Mode Flags**:
- `-mode vec`: Enable vector benchmark mode
- `-uri`: Server URI (default: grpc://127.0.0.1:3000)
- `-dim`: Vector dimension (default: 128)
- `-dtype`: Data type (default: float32)
- `-scale`: Number of vectors to ingest (default: 1000)
- `-queries`: Number of search queries (default: 1000)
- `-search-modes`: Comma-separated modes (default: dense,hybrid,sparse,filtered,byid)
- `-dataset`: Dataset name (default: benchmark)
- `-json`: Output JSON file path

**Usage**:
  ```bash
  # Start server first
  ./bin/longbow &
  
  # Run vector benchmark
  ./bin/bench-tool -mode vec -dim 128 -scale 5000 -queries 1000 -search-modes dense
  
  # Run with all search modes and JSON output
  ./bin/bench-tool -mode vec -dim 384 -scale 10000 -search-modes dense,hybrid,sparse,filtered,byid -json results.json
  ```

**Note**: Requires running Longbow server. Use `scripts/bench_functional_test.sh` for automated testing.

### longbow-cli (bin/longbow)

User-facing CLI for functional operations. Located at `bin/longbow`.

**Commands**:
- `search`: Dense, sparse, filtered, hybrid search
- `geo-search`: Geospatial search with radius
- `temporal-search`: Temporal index queries
- `import`: Load Parquet/NumPy files
- `create-dataset`, `delete`, `snapshot`: Dataset management

**Usage**:
  ```bash
  ./bin/longbow search -dataset mydata -mode dense -vector 0.1,0.2,0.3 -k 10
  ./bin/longbow import -dataset mydata ./data.parquet
  ```

Install Python dependencies before running any Python tools, or Go tools for bench-tool:

```bash
# Python dependencies
pip install -r scripts/requirements.txt

# Build Go tools
go build -o bin/bench-tool ./cmd/bench_io
go build -o bin/longbow ./cmd/longbow
```

## Testing

### Unit Tests

Run Go unit and fuzz tests for bench-tool:

```bash
# All tests
go test -v ./cmd/bench_io/...

# With race detector
go test -race ./cmd/bench_io/...

# Fuzz tests
go test -fuzz ./cmd/bench_io/... -fuzztime 60s
```

Run CLI tests:

```bash
go test -v ./cmd/cli/...
```

### Functional Tests

Automated functional testing:

```bash
# bench-tool functional tests (starts server automatically)
./scripts/bench_functional_test.sh

# GraphRAG functional tests
python3 scripts/graph_functional_test.py

# CLI verification
./scripts/test_cli.sh
```

### Performance Tests

Full benchmark matrix:

```bash
# Unified benchmark (CPU/Metal/CUDA)
python3 scripts/unified_benchmark.py --mode cpu --dims 128,384 --counts 1000,5000

# Sequential benchmark
python3 scripts/seq_benchmark.py --mode cpu --dim 128 --count 10000

# Local benchmarks
./scripts/run_all_local.sh
```

## Notes

Install Python dependencies before running any Python tools:

```bash
pip install -r scripts/requirements.txt
```

## Notes

- Most scripts expect a running Longbow instance or cluster
- Default ports: 3000 (data), 3001 (meta), 9090 (metrics)
- Generated logs and profiles are gitignored
- Results are saved to `data/perf_logs/perf_matrix_*.json`