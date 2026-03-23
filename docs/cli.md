# Longbow CLI

Command-line interface for Longbow vector store management.

## Installation

```bash
go build -o bin/longbow-cli ./cmd/cli
```

## Commands

### import

Import data into a dataset.

```bash
longbow-cli import -dataset <name> -input <file> [-dim <n>] [-count <n>]
```

Options:
- `-dataset` - Target dataset name (required)
- `-input` - Input file path (optional, generates demo data if omitted)
- `-dim` - Vector dimension (default: 128)
- `-count` - Number of vectors to generate (default: 1000)

### search

Search vectors with different modes.

```bash
longbow-cli search -dataset <name> -mode <mode> [options]
```

Options:
- `-dataset` - Dataset name (required)
- `-mode` - Search mode: dense, sparse, filtered, hybrid (default: dense)
- `-vector` - Query vector as comma-separated floats
- `-text` - Text query for sparse/hybrid search
- `-alpha` - Alpha for hybrid search (0=sparse, 1=dense, default: 0.5)
- `-k` - Number of results (default: 10)
- `-filters` - JSON filter expression (inline or file path)

### Search Modes

#### Dense Search

```bash
longbow-cli search -dataset mydata -mode dense -vector "0.1,0.2,0.3" -k 10
```

#### Sparse Search

```bash
longbow-cli search -dataset mydata -mode sparse -text "search query" -k 10
```

#### Filtered Search with Compound Expressions

```bash
longbow-cli search -dataset mydata -mode filtered -vector "0.1,0.2" \
  -filters '{
    "logic": "AND",
    "filters": [
      {"field": "id", "operator": ">", "value": "10"},
      {"logic": "OR", "filters": [
        {"field": "category", "operator": "=", "value": "1"},
        {"field": "status", "operator": "=", "value": "2"}
      ]}
    ]
  }'
```

#### Hybrid Search

```bash
longbow-cli search -dataset mydata -mode hybrid -vector "0.1,0.2" -text "query" -alpha 0.5
```

### Dataset Management

#### Create Namespace

```bash
longbow-cli create-namespace -name mydata
```

#### Delete Namespace

```bash
longbow-cli delete-namespace -name mydata
```

#### List Namespaces

```bash
longbow-cli list-namespaces
```

#### Dataset Stats

```bash
longbow-cli stats -dataset mydata
```

## Compound Filter Expressions

Filters support AND, OR, NOT logic with nested field paths:

```json
{
  "logic": "AND",
  "filters": [
    {"field": "metadata.status", "operator": "=", "value": "active"},
    {"logic": "OR", "filters": [
      {"field": "tags.item", "operator": "=", "value": "premium"},
      {"field": "score", "operator": ">", "value": "80"}
    ]}
  ]
}
```

Supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`

Supported field paths: flat fields, nested struct fields (dot notation), list items
