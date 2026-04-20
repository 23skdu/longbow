# Longbow Scripts Directory

This directory contains consolidated utility tools for testing, benchmarking, and deploying Longbow.

## 🛠️ Main Tools

### `benchmark_tool.py` (Performance & Benchmarking)
A unified Python CLI for performance testing and documentation.
- **`run`**: Execute exhaustive benchmarks (CPU, Metal, CUDA).
- **`summary`**: Aggregates the latest JSON results into a readable summary.
- **`report`**: Generates `docs/performance.md` from test results.
- **`pushdown`**: Runs specialized filter/projection pushdown benchmarks.

### `cluster_tool.sh` (Infrastructure & Ops)
A shell utility for Kubernetes environment management and resilience testing.
- **`up`**: Provision a local Kind cluster.
- **`deploy`**: Build and load Docker images into the cluster.
- **`test-dist`**: Run distributed testing scenarios across multiple namespaces.
- **`chaos-partition`**: Simulate a network partition (isolates a node).
- **`chaos-heal`**: Resolve active partitions.
- **`down`**: Tear down the cluster.

### `data_tool.py` (Data & Demos)
Integrated utility for data seeding and feature demonstrations.
- **`seed`**: Add graph edges to a dataset.
- **`lorem`**: Large-scale NLP vector search benchmarking with Lorem Ipsum data.
- **`rag`**: GraphRAG embedding demonstration.
- **`metrics`**: Showcase distance metric comparisons.

### `dashboard_tool.py` (Observability)
Automates the generation and enhancement of Grafana dashboards.
- **`enhance`**: Adds comprehensive observability panels to `grafana/dashboards/longbow.json`.

## 📁 Subdirectories

- **`ci/`**: Continuous Integration scripts (e.g., `build.sh`).
- **`dev/`**: Development environment helpers (e.g., `dev.sh`).
- **`chaos_tools/`**: Low-level chaos triggers.

## 📦 Requirements

Install Python dependencies before running the tools:

```bash
pip install -r scripts/requirements.txt
```

## 📝 Notes
- Most scripts expect a running Longbow instance or cluster.
- Default ports: 3000 (data), 3001 (meta), 9090 (metrics).
- Generated logs and profiles are gitignored.
