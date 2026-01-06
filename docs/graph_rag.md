# GraphRAG and Similarity Search

Longbow supports Knowledge Graph (GraphRAG) capabilities alongside vector search, allowing for explicit relationships between vectors (nodes) and advanced traversal.

## GraphRAG Concepts

The GraphRAG system overlays a directed graph on top of the vector index.

- **Node**: Corresponds to a stored Vector. Nodes are referred to by their User ID or Internal ID.
- **Edge**: A directed connection between two nodes with a `predicate` (label) and `weight`.

## Usage

### 1. Adding Edges

Edges are added using the Flight Action `add-edge`.

**CLI Example:**

```bash
# Add a "knows" relationship from ID 100 to ID 101
python3 scripts/ops_test.py add-edge \
  --dataset my_dataset \
  --subject 100 \
  --predicate knows \
  --object 101 \
  --weight 1.0
```

### 2. Graph Connectivity Stats

Retrieve statistics about the graph structure using `graph-stats`.

**CLI Example:**

```bash
python3 scripts/ops_test.py graph-stats --dataset my_dataset
```

### 3. Graph Traversal

Traverse the graph starting from a node to find connected entities.

**CLI Arguments:**

- `--max-hops`: Maximum depth of traversal (default: 2).
- `--incoming`: Traverse incoming edges instead of outgoing.
- `--no-weighted`: Disable edge weight consideration (BFS).
- `--decay`: Apply weight decay factor (e.g., 0.5) per hop.

**CLI Example:**

```bash
python3 scripts/ops_test.py traverse \
  --dataset my_dataset \
  --start-node 100 \
  --max-hops 3
```

## Similarity Search By ID

You can find vectors similar to a specific item given its ID using the `similar` command. This performs a lookup of the vector associated with the ID and then runs a standard similarity search.

**CLI Example:**

```bash
python3 scripts/ops_test.py similar \
  --dataset my_dataset \
  --id 100 \
  --k 5
```
