# Longbow Scripts Directory

This directory contains utility tools for testing, benchmarking, and deploying Longbow.

## 🛠️ Main Tools

### `unified_benchmark.py` (Performance & Benchmarking)
The primary benchmark tool for Longbow performance testing.
- **Modes**: `cpu`, `metal`, `cuda`, `onnx`, `recommend`, `deletion`, `graphrag`, `exchange`, `cluster`, `temporal`
- **Usage**:
  ```bash
  python3 scripts/unified_benchmark.py --mode cpu
  python3 scripts/unified_benchmark.py --mode metal
  python3 scripts/unified_benchmark.py --dims 128,384,768 --counts 1000,5000,10000 --dtypes float32,int8,complex64
  ```
- Supports exhaustive matrix testing across dimensions, data types, and batch sizes
- Generates JSON results and Markdown reports automatically

### `cluster_tool.sh` (Infrastructure & Ops)
A shell utility for Kubernetes environment management and resilience testing.
- **`up`**: Provision a local Kind cluster.
- **`deploy`**: Build and load Docker images into the cluster.
- **`test-dist`**: Run distributed testing scenarios across multiple namespaces.
- **`chaos-partition`**: Simulate a network partition (isolates a node).
- **`chaos-heal`**: Resolve active partitions.
- **`down`**: Tear down the cluster.

### `bench-tool` (Go Benchmark Binary)
Located at `cmd/bench-tool/main.go`, this Go binary performs ingest and search benchmarking against a running Longbow server.
- Connects to Longbow via gRPC
- Supports all data types: float32, float16, int8, int64, complex64, complex128, turboquant
- Outputs JSON results for programmatic analysis


## 📦 Requirements

Install Python dependencies before running the tools:

```bash
pip install -r scripts/requirements.txt
```

## 📝 Notes

- Most scripts expect a running Longbow instance or cluster.
- Default ports: 3000 (data), 3001 (meta), 9090 (metrics).
- Generated logs and profiles are gitignored.
- Results are saved to `data/perf_logs/perf_matrix_*.json`
