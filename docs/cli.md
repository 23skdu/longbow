# Longbow CLI Reference

The Longbow CLI is a powerful administrative tool for managing datasets, namespaces, and performing vector searches directly from the terminal.

## Installation

To build the CLI from source, ensure you have Go 1.24+ installed and run:

```bash
go build -o bin/longbow-cli ./cmd/cli
```

The binary will be created in the `bin/` directory.

## Global Options

All commands support the following global options:

- `-uri string`: Longbow server URI (default: `grpc://127.0.0.1:3000`)

---

## Commands

### 1. Import Data
Import vectors from Parquet, NumPy, or generate demo data. Supports local filesystem and remote S3 buckets.

**Usage:**
`longbow-cli import -dataset <name> [options]`

**Options:**
- `-dataset string`: Target dataset name (required)
- `-input string`: Path to `.parquet`, `.npy`, or `s3://bucket/key` (required)
- `-dim int`: Vector dimension (default: 128, used for demo data)
- `-count int`: Number of vectors to generate (default: 1000, used for demo data)

**Example:**
```bash
# Local file
longbow-cli import -dataset my-collection -input data.parquet

# S3 bucket
longbow-cli import -dataset my-collection -input s3://my-bucket/vectors.parquet
```

### 2. Search Commands

#### Vector Search
Perform high-performance vector searches using various modes.

**Usage:**
`longbow-cli search -dataset <name> -mode <type> [options]`

**Options:**
- `-dataset string`: Dataset name (required)
- `-mode string`: Search mode (dense, sparse, filtered, hybrid)
- `-vector string`: Query vector as comma-separated floats
- `-text string`: Text query for sparse/hybrid search
- `-alpha float`: Hybrid weighting (0=sparse, 1=dense)
- `-k int`: Number of results to return

#### Geospatial Search
Search for vectors within a physical radius.

**Usage:**
`longbow-cli geo-search -dataset <name> -lat <val> -lon <val> -radius <km> -k <n>`

#### Recommendations
Get similar vectors based on existing IDs.

**Usage:**
`longbow-cli recommend -dataset <name> -seeds <id1,id2> -k <n> -alpha <f>`

### 3. Namespace & Dataset Management
Manage logical groupings and lifecycle of data.

- **Create Namespace:** `longbow-cli create-namespace -name <name>`
- **Delete Namespace:** `longbow-cli delete-namespace -name <name>`
- **List Namespaces:** `longbow-cli list-namespaces`
- **List Datasets:** `longbow-cli list-datasets-in-namespace -namespace <name>`
- **Delete ID:** `longbow-cli delete -dataset <name> -id <id>`
- **Snapshot:** `longbow-cli snapshot` (Triggers manual disk flush)
- **Stats:** `longbow-cli stats -dataset <name>`

### 4. Graph & GraphRAG Operations
Administrative tools for managing the HNSW graph as a knowledge graph.

- **Add Edge:** `longbow-cli add-edge -dataset <ds> -subject <id> -predicate <p> -object <id> -weight <f>`
- **Traverse:** `longbow-cli traverse -dataset <ds> -start <id> -hops <n>`
- **Graph Stats:** `longbow-cli get-graph-stats -dataset <ds>`
- **PageRank:** `longbow-cli pagerank -dataset <ds> -iterations <n>`
- **Community Detection:** `longbow-cli detect-communities -dataset <ds>`

### 5. Temporal Search
Query the temporal index for versioned data.

**Usage:**
`longbow-cli temporal-search -type <as_of|range|window> [options]`

**Options:**
- `-ts int`: Unix nanosecond timestamp for `as_of`
- `-start int`: Start time for `range`
- `-end int`: End time for `range`

---

## Advanced Filtering

The `-filters` flag in `search` accepts a JSON object representing complex boolean logic:

```json
{
  "logic": "AND",
  "filters": [
    {"field": "category", "operator": "=", "value": "electronics"},
    {"field": "price", "operator": "<", "value": "100"}
  ]
}
```
