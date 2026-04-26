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
Import vectors from Parquet, NumPy, or generate demo data.

**Usage:**
`longbow-cli import -dataset <name> [options]`

**Options:**
- `-dataset string`: Target dataset name (required)
- `-input string`: Path to `.parquet` or `.npy` file
- `-dim int`: Vector dimension (default: 128, used for demo data)
- `-count int`: Number of vectors to generate (default: 1000, used for demo data)

**Example:**
```bash
longbow-cli import -dataset my-collection -input data.parquet
```

### 2. Vector Search
Perform high-performance vector searches using various modes.

**Usage:**
`longbow-cli search -dataset <name> -mode <type> [options]`

**Search Modes:**
- `dense`: Standard L2/Cosine search on dense vectors
- `sparse`: Keyword-based search using sparse indices
- `filtered`: Metadata-filtered vector search
- `hybrid`: Combined dense and sparse search with alpha weighting

**Options:**
- `-dataset string`: Dataset name (required)
- `-mode string`: Search mode (default: `dense`)
- `-vector string`: Query vector as comma-separated floats (e.g., "0.1,0.2,...")
- `-text string`: Text query for sparse/hybrid search
- `-alpha float`: Hybrid weighting (0=sparse, 1=dense, default: 0.5)
- `-k int`: Number of results to return (default: 10)
- `-filters string`: JSON filter expression (string or file path)

**Example:**
```bash
longbow-cli search -dataset my-collection -mode hybrid -vector "0.1,0.5" -text "example query" -k 5
```

### 3. Namespace Management
Manage logical groupings of datasets.

- **Create Namespace:** `longbow-cli create-namespace -name <name>`
- **Delete Namespace:** `longbow-cli delete-namespace -name <name>`
- **List Namespaces:** `longbow-cli list-namespaces`
- **List Datasets:** `longbow-cli list-datasets-in-namespace -namespace <name>`

### 4. Dataset Statistics
Show operational statistics for a dataset.

**Usage:**
`longbow-cli stats -dataset <name>`

**Output includes:**
- Index size and memory usage
- Vector count
- Health status
- Training state

---

## Advanced Filtering

The `-filters` flag accepts a JSON object representing complex boolean logic:

```json
{
  "logic": "AND",
  "filters": [
    {"field": "category", "operator": "=", "value": "electronics"},
    {"field": "price", "operator": "<", "value": "100"}
  ]
}
```

You can pass this JSON directly or provide a path to a `.json` file.
