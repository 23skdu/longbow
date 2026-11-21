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

## Authentication, RBAC & OIDC Federation

`LongbowClient` supports API keys, federated OIDC JWTs (direct Bearer auth or short-lived session token exchange), tenant/namespace scoping, and mTLS:

```python
from longbow import (
    LongbowClient,
    LongbowAuthenticationError,
    LongbowAuthorizationError,
)

# 1. Authenticate with an RBAC API key (scoped to tenant/namespace)
admin = LongbowClient(
    uri="grpc://localhost:3000",
    meta_uri="grpc://localhost:3001",
    api_key="lb_admin_secret",
    tenant_id="acme",
    namespace="acme",
)

# Issue scoped keys for any role: admin, tenant-admin, writer (read-write), ingest-only, reader (read-only)
ingest_key = admin.create_api_key(
    name="etl-ingestor",
    role="ingest-only",
    tenant_id="acme",
    namespace="acme",
    datasets=["acme/docs"],
    row_filters={"department": "engineering"},
    ttl_seconds=86400,
)
admin.rotate_api_key(ingest_key["api_key"], grace_period_seconds=3600)
admin.set_rls_policy("eng-only", "acme", "docs", [{"field": "department", "op": "eq", "values": ["engineering"]}])
keys = admin.list_api_keys()
audit_entries = admin.get_audit_log(limit=50)

# 2. Register AWS IRSA / Google IAM / Generic OIDC providers (Flight or REST v1)
admin.add_google_oidc_provider(audience="longbow")
admin.add_aws_oidc_provider(
    issuer_url="https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
    audience="sts.amazonaws.com",
    trust_policy_arn="arn:aws:iam::123456789012:role/longbow-reader",
)
providers = admin.list_oidc_providers()

# 3. Authenticate workloads with OIDC (direct JWT or short-lived session exchange)
workload = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001", tenant_id="acme")
workload.exchange_oidc_token(oidc_jwt, ttl_seconds=900)  # auto-binds session key to workload.api_key
# Or acquire + exchange automatically on AWS EKS / GCP GCE/GKE:
# workload.authenticate_with_aws_oidc(ttl_seconds=900)
# workload.authenticate_with_google_oidc(audience="longbow", ttl_seconds=900)

# 4. Typed Auth Error Handling (401 vs 403 / gRPC Unauthenticated vs PermissionDenied)
try:
    workload.search("other_tenant/secrets", vector=[0.1] * 128, k=5)
except LongbowAuthorizationError as e:
    # HTTP 403 / gRPC PERMISSION_DENIED (role, namespace isolation, or dataset allow-list)
    print(f"Access denied ({e.code}): {e}")
except LongbowAuthenticationError as e:
    # HTTP 401 / gRPC UNAUTHENTICATED (invalid/expired/revoked key or JWT replay/expiry)
    print(f"Authentication failed ({e.code}): {e}")
```

See [docs/rbac.md](../docs/rbac.md) for the complete RBAC role/permission matrix, dataset access rules, and OIDC claim mapping.
