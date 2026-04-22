# Longbow Arrow Flight API Functions

This document details the Arrow Flight operations supported by Longbow's **Data** and **Meta**
servers, including `DoPut`, `DoGet`, `DoExchange`, and specific `DoAction` commands.

It also maps these functions to the `scripts/ops_test.py` CLI tool, which serves as the primary functional testing utility.

## Overview

- **Data Plane (Port 3000)**: Optimized for raw throughput and local node operations. Handles:
  - **Ingestion (`DoPut`)**: Standard way to write data.
  - **Bulk Scan (`DoGet`)**: Full dataset retrieval with optional filters.
  - **Local Search (`DoGet`)**: Single-node vector/hybrid search.
- **Meta/Query Plane (Port 3001)**: Optimized for coordination and global visibility. Handles:
  - **Global Search (`DoGet`)**: Distributed search across the entire cluster (automatically handles scatter-gather).
  - **Advanced Analytics (`DoAction`)**: PageRank, Community Detection, and Graph Traversal.
  - **Management (`DoAction`)**: Namespaces, Cluster Status, and Dataset deletion.
  - **CDC & Discovery**: Coordination of change data capture and mesh membership.

---

## 1. Data Plane (`DataServer` - Port 3000)

### `DoPut` (Ingestion)

Stream Arrow RecordBatches to ingest data into a dataset.

- **Behavior**: Auto-creates dataset if missing. Supports batched writes, WAL logging, and async indexing.
- **Backpressure**: Signals `slow_down` metadata if WAL queue > 80%.
- **CLI Usage**:

  ```bash
  # Upload 1000 rows to 'my_dataset'
  python3 scripts/ops_test.py put --dataset my_dataset --rows 1000
  ```

### `DoGet` (Bulk Retrieval)

Stream all records from a dataset.

- **Input**: Ticket containing JSON `{"name": "dataset_name", "filters": [...]}`.
- **Output**: Stream of Arrow RecordBatches.
- **CLI Usage**:

  ```bash
  # Download 'my_dataset'
  python3 scripts/ops_test.py get --dataset my_dataset
  ```

### `DoExchange` (Bidirectional Stream)

Used for synchronization and advanced bidirectional protocols.

- **Current Support**: verification echo/fetch.
- **CLI Usage**:

  ```bash
  python3 scripts/ops_test.py exchange
  ```

---

## 2. Control & Query Plane (`MetaServer` - Port 3001)

### `DoGet` (Global Search)

Performs distributed Vector or Hybrid search across the entire cluster.

> [!IMPORTANT]
> `DoPut` (Ingestion) is **DISABLED** on the Meta/Query port to prevent control-plane saturation. All data ingestion MUST target Port 3000.

- **Input**: Ticket containing JSON wrapped in "search" key:

  ```json
  {
    "search": {
      "dataset": "name",
      "vector": [...],
      "text_query": "optional", 
      "k": 10,
      "graph_alpha": 0.5,
      "graph_depth": 2
    }
  }
  ```

- **Output**: Arrow RecordBatch stream with `id` (uint64) and `score` (float32).
- **CLI Usage**:

  ```bash
  # Vector Search
  python3 scripts/ops_test.py search --dataset my_dataset --k 5
  
  # Hybrid Search
  python3 scripts/ops_test.py search --dataset my_dataset --text-query "apple" --alpha 0.5
  ```

### `DoAction` (Management & Operations)

Executes specific control commands.

#### Cluster & Mesh

| Action Type | Description | CLI Command |
| :--- | :--- | :--- |
| `cluster-status` | Get node identity and member list. | `python3 scripts/ops_test.py status` |
| `MeshIdentity` | Get local node identity. | (Internal/Status) |
| `MeshStatus` | Get mesh membership list. | (Internal/Status) |
| `DiscoveryStatus` | Get peer discovery diagnostics. | (Internal/Status) |

#### Dataset Operations

| Action Type | Description | CLI Command |
| :--- | :--- | :--- |
| `delete` | Soft-delete specific vector ID. | `python3 scripts/ops_test.py delete --dataset <name> --ids 1,2` |
| `delete-dataset` | Permanently delete dataset. | (Used in `validate` or `scripts/cleanup.py`) |
| `delete-vector` | Delete by internal VectorID. | (Internal/Advanced) |
| `VectorSearchByID` | Find similar vectors to a given ID. | `python3 scripts/ops_test.py similar --dataset <name> --id <ID>` |

#### Namespace Management

| Action Type | Description | CLI Command |
| :--- | :--- | :--- |
| `CreateNamespace` | Create a new namespace. | `python3 scripts/ops_test.py namespaces` |
| `DeleteNamespace` | Delete a namespace. | `python3 scripts/ops_test.py namespaces` |
| `ListNamespaces` | List all namespaces. | `python3 scripts/ops_test.py namespaces` |
| `GetTotalNamespaceCount`| Count total namespaces. | `python3 scripts/ops_test.py namespaces` |
| `GetNamespaceDatasetCount`| Count datasets in a namespace. | `python3 scripts/ops_test.py namespaces` |

#### GraphRAG (Knowledge Graph)

| Action Type | Description | CLI Command |
| :--- | :--- | :--- |
| `add-edge` | Add semantic edge (Subject->Predicate->Object). | `python3 scripts/ops_test.py add-edge ...` |
| `traverse-graph` | Traverse graph from start node. | `python3 scripts/ops_test.py traverse ...` |
| `calculate-pagerank`| Compute importance scores for nodes. | `scripts/graph_functional_test.py` |
| `detect-communities`| Group nodes into clusters based on topology. | `scripts/graph_functional_test.py` |
| `GetGraphStats` | Get graph statistics (nodes, edges). | `python3 scripts/ops_test.py graph-stats ...` |

#### System

| Action Type | Description | CLI Command |
| :--- | :--- | :--- |
| `ForceSnapshot` | Force database snapshot to disk (Reserved/Planned). | `python3 scripts/ops_test.py snapshot` |
| `VectorSearch` | Unary vector search (alternative to DoGet). | (Internal) |

---

## 3. Testing & Benchmarking Scripts

### `scripts/ops_test.py`

The primary functional CLI tool. Use this for:

- Manual testing of all features.
- CI/CD integration smoke tests (`validate` subcommand).
- Debugging specific operations.

**Examples:**

```bash
# Run full smoke test suite
python3 scripts/ops_test.py validate

# Inspect cluster membership
python3 scripts/ops_test.py status
```

### `scripts/perf_test.py`

The high-concurrency benchmarking tool. Use this for:

- Throughput/Latency testing.
- Load testing (Soak tests).
- Measuring ingestion speed.

**Examples:**

```bash
# Run standard benchmark (10k rows, 128 dim)
python3 scripts/perf_test.py --rows 10000 --dim 128

# Run Hybrid Search benchmark
python3 scripts/perf_test.py --hybrid --search
```
