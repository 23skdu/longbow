# Reciprocal Rank Fusion (RRF) & Distributed Global RRF

---

## 1. What is Reciprocal Rank Fusion (RRF)?

Reciprocal Rank Fusion (RRF) is a state-of-the-art rank aggregation algorithm used to combine multiple ranked result lists (e.g., from dense vector search and sparse keyword search) into a single, unified relevance ranking without requiring score normalization.

Because dense vector similarity scores (e.g., Cosine, L2) and sparse scores (e.g., BM25) operate on completely different numerical scales, direct score-based merging is highly unstable. RRF solves this by ignoring raw scores entirely and focusing purely on the relative rank positions of the documents.

### How RRF Works

For a set of ranked lists, the RRF score for a document $d$ is calculated as:

$$RRF\_Score(d) = \sum_{m \in M} \frac{1}{RRF\_Constant + Rank_m(d)}$$

Where:

- $M$: The set of input ranking models (e.g., dense and sparse).
- $Rank_m(d)$: The 1-based index position of document $d$ in the output of model $m$. If a document does not appear in a list, its rank score for that model is 0.
- $RRF\_Constant$: A smoothing constant (traditionally denoted as $k$, defaulting to `60` in Longbow) that prevents highly-ranked documents from completely dominating while smoothing the penalization of lower ranks.

---

## 2. Distributed Global Reciprocal Rank Fusion

In a distributed, sharded search architecture, traditional RRF exhibits a major regression: **local rank skew**.

If each cluster node executes dense and sparse searches on its own local shard and applies RRF locally, the local ranks are highly distorted because the node only has visibility into a fraction of the corpus. When the coordinator attempts to merge these pre-fused lists, the aggregate rank is mathematically incorrect.

### The Longbow Architecture: Global RRF

Longbow solves this regression by implementing **Global Reciprocal Rank Fusion** inside [global_search.go](file:///Users/rsd/REPOS/longbow/internal/store/global_search.go):

1. **Raw Scatter**: The coordinator scatters the hybrid search request to all cluster nodes, requesting *un-fused* raw dense and sparse candidates.
2. **Global Gather**: All raw candidate lists are streamed back to the coordinator using zero-copy Arrow Flight streams.
3. **Global Sort**: The coordinator aggregates all dense candidates into a single global dense list, and all sparse candidates into a single global sparse list, sorting each by score to establish a **true global rank**.
4. **Global Fusion**: The coordinator executes the RRF calculation on these globally ranked lists:

   ```go
   finalResults = ReciprocalRankFusion(req.Dataset, allDense, allSparse, 60, req.K, nil)
   ```

---

## 3. Implementing RRF with WASM / ONNX Models

Longbow's **Unified ML Inference Engine** allows developers to embed lightweight transformer models (e.g., embedding and sparse keyword generators) directly into the database nodes via pure-Go WebAssembly (`wazero`).

### In-Process Ingestion & Search Pipeline

1. **WASM Embedding**: Download a model (e.g., `all-MiniLM-L6-v2`) to a database node using the administrative command:

   ```bash
   longbow-cli download-model -repo sentence-transformers/all-MiniLM-L6-v2 -dest models/all-mini
   ```

2. **Dense & Sparse Inference**: The in-process WASM/ONNX runtime executes the model to generate:

   - A **Dense vector** (e.g., 384-dimensional float array).
   - A **Sparse bag-of-words vector** (e.g., SPLADE token allocations or BM25 query terms).

3. **Execution**: Both vectors are submitted to [hybrid_search.go](file:///Users/rsd/REPOS/longbow/internal/store/hybrid_search.go), performing HNSW graph traversal and inverted index block-max WAND matching simultaneously.
4. **RRF Aggregation**: The results are instantly fused using [ReciprocalRankFusion](file:///Users/rsd/REPOS/longbow/internal/store/hybrid_search.go#L254-L300) before being returned to the client.

---

## 4. How to Use RRF in Longbow

### Using the Longbow CLI (`longbow-cli`)

Perform hybrid search using RRF by choosing `hybrid` search mode. Use `-alpha` to configure how heavily dense or sparse should influence candidates, and `-text` for the textual query:

```bash
longbow-cli search \
  -uri grpc://127.0.0.1:3000 \
  -dataset product-catalog \
  -mode hybrid \
  -text "wireless noise cancelling headphones" \
  -alpha 0.5 \
  -k 10
```

### Using the Python SDK (`pythonsdk`)

The Python SDK handles zero-copy Arrow-backed retrieval and maps the request to the distributed RRF fusion coordinator:

```python
from longbow import LongbowClient

# Initialize the client
client = LongbowClient("grpc://127.0.0.1:3000")

# Perform hybrid reciprocal rank fusion search
results_df = client.search(
    dataset="product-catalog",
    vector=[0.12, 0.43, -0.05, ...], # Dense vector
    text="wireless noise cancelling headphones", # Sparse text query
    mode="hybrid",
    alpha=0.5, # Balance parameter
    k=10
)

# results_df is a Pandas DataFrame ordered by fused RRF scores
print(results_df[["id", "score"]])
```

---

## 5. Use Cases

- **E-Commerce Search**: Combines exact product name matching (sparse keyword) with semantic user intent (dense vector), ensuring exact-phrase matches (e.g., SKU numbers) do not get lost in semantic clustering.
- **Enterprise Q&A (RAG)**: Integrates exact acronyms, department names, or code functions (sparse) with natural language questions (dense) to supply highly relevant context to LLM prompts.
- **Cross-Lingual Search**: Uses dense joint spaces for semantic translation alignment alongside sparse dictionary indices to boost exact terms that match across languages.
