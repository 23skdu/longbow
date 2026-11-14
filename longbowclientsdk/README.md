# Longbow Python SDK

The official Python client for [Longbow](https://github.com/23skdu/longbow), a high-performance vector engine.

## Features

- **High-Performance Ingestion**: Native Arrow Flight support for bulk loading and streaming.
- **All Search Modalities**: Dense ANN, ByID (`search_by_id`), Sparse BM25, Advanced Full-Text (`advanced_text_search`, `search_facets`, `tune_bm25`, `search_analytics`), Hybrid RRF, Distance Range Search (`range_search`), Temporal (`temporal_search`), Geo-Spatial (`geo_search`), Multi-Modal (`ingest_media`, `ingest_media_parquet`, `multimodal_search`), Hybrid Vector-Graph Recommendation (`recommend`), GraphRAG (`traverse`, `graph_rag_expand`), and Federated Cross-Collection Search (`federated_search`).
- **Search Pipeline & Explainability**: Cross-encoder/LLM/MMR reranking (`rerank`, `evaluate_reranker`), query rewriting (`rewrite_query`), cost-based plan inspection (`explain_query_plan`), HNSW hop/score explainability (`explain_search`, `debug_search_path`, `plot_search_explanation`), and vector provenance (`record_provenance`, `get_provenance`).
- **Enterprise Administration**: RBAC/RLS/Audit (`create_api_key`, `rotate_api_key`, `revoke_api_key`, `list_api_keys`, `set_rls_policy`, `get_audit_log`), online backups & PITR (`trigger_backup`, `list_backups`, `verify_backup`, `restore_backup`), CDC & streaming connectors (`cdc_status`, `create_stream_connector`, `ingest_stream_event`), tenant metering & quotas (`get_tenant_usage`, `set_tenant_quota`, `export_billing_report`), elastic sharding (`get_cluster_topology`, `trigger_shard_split`, `trigger_shard_merge`, `trigger_rebalance`), schema registry (`register_schema`, `validate_schema`, `migrate_schema`), materialized views (`create_materialized_view`, `query_materialized_view`), and Managed Cloud / REST v1 / GraphQL helpers.
- **Dask & Pandas Integration**: Seamlessly handle large-scale datasets with Dask or Pandas DataFrames.
- **Type Safety**: Pydantic models for request/response validation.

## Installation

```bash
pip install longbow
```

## Quick Start

```python
from longbow import LongbowClient
import dask.dataframe as dd
import pandas as pd
import numpy as np

# Connect to Longbow (Data Port 3000, Meta Port 3001)
client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")

# Create generic data
df = pd.DataFrame({
    "id": range(1000),
    "vector": [np.random.rand(128).tolist() for _ in range(1000)],
    "metadata": ["test" for _ in range(1000)]
})
ddf = dd.from_pandas(df, npartitions=2)

# Ingest Dask DataFrame
client.insert("my_dataset", ddf)

# Search
results = client.search(
    dataset="my_dataset",
    vector=np.random.rand(128).tolist(),
    k=5
)
print(results.compute()) # Materialize Dask result
```
