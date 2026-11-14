# Longbow CLI Reference (`longbow-cli`)

`longbow-cli` (`cmd/cli/main.go`, `cmd/cli/cloud_api_cmd.go`) is the unified command-line administration, ingestion, search, security, streaming, sharding, and cloud control-plane client for Longbow. It communicates over both Apache Arrow Flight (gRPC) and HTTP REST/GraphQL endpoints.

---

## 1. Installation & Global Configuration

Build the binary from source:

```bash
go build -o bin/longbow-cli ./cmd/cli
```

### Environment Variables & Common Flags

| Flag / Variable | Default | Description |
| :--- | :--- | :--- |
| `-uri` | `grpc://127.0.0.1:3000` (Flight) or `http://127.0.0.1:8080` (HTTP) | Target Longbow server endpoint |
| `LONGBOW_API_KEY` | `""` | Bearer / `X-API-Key` token automatically attached to gRPC metadata and HTTP headers when set |

---

## 2. Dataset, Namespace & Model Lifecycle

| Command | Flags | Description |
| :--- | :--- | :--- |
| `import` | `-dataset <ds> [-input <path/s3/gs>] [-dim 128] [-count 1000]` | Ingest Parquet, NumPy `.npy`, S3/GCS objects, or synthetic demo vectors |
| `export` | `-dataset <ds> -file <path/s3/gs>` | Export a dataset to local Arrow/Parquet or S3/GCS |
| `create-dataset` | `-name <ds> [-dims 128] [-type float32] [-geo]` | Create a dataset with explicit dimensionality, data type, and optional geo index |
| `drop` | `-dataset <ds>` | Drop a dataset and reclaim off-heap slab memory |
| `delete` | `-dataset <ds> -id <id>` | Delete a vector by ID (marks tombstone) |
| `stats` | `[-dataset <ds>]` | Display vector count, index status, and memory statistics |
| `snapshot` | `-uri <uri>` | Trigger an immediate disk snapshot flush (`ForceSnapshot`) |
| `create-namespace` | `-name <ns> [-dims 0] [-data_type float32]` | Create an isolated namespace |
| `delete-namespace` | `-name <ns>` | Delete a namespace and all datasets inside it |
| `list-namespaces` | `-uri <uri>` | List all namespaces |
| `list-datasets-in-namespace` | `-namespace <ns>` | List datasets belonging to a namespace |
| `download-model` | `-repo <hf-repo> [-dest models/<name>]` | Download an ONNX model, `config.json`, and `vocab.txt` from Hugging Face |

```bash
# Generate 10,000 synthetic 384D vectors
longbow-cli import -dataset demo -dim 384 -count 10000

# Create a TurboQuant dataset
longbow-cli create-dataset -name compressed_docs -dims 768 -type turboquant
```

---

## 3. Search, Discovery & Query Pipeline

### Core & Specialized Search Modes

| Command | Flags | Description |
| :--- | :--- | :--- |
| `search` | `-dataset <ds> -mode <dense\|sparse\|filtered\|hybrid\|by-id> [-vector f1,f2...] [-id <id>] [-text <q>] [-alpha 0.5] [-k 10] [-filters <json>]` | Execute Dense ANN, Sparse BM25, Filtered, Hybrid RRF, or ByID search |
| `range-search` | `-dataset <ds> -vector <f1,f2...> -threshold <dist> [-k 100] [-filters <json>]` | Return all vectors within a distance threshold (`RangeSearch`) |
| `text-search` | `-dataset <ds> -query <expr> [-k 10] [-fuzzy 0] [-facets f1,f2]` | Boolean AST (`AND`/`OR`/`NOT`), phrase, and fuzzy (`~N`) BM25 search with `<mark>` snippets |
| `tune-bm25` | `-dataset <ds> [-k1 1.2] [-b 0.75]` | Dynamically tune per-dataset BM25 `k1` and `b` parameters (`TuneBM25`) |
| `search-facets` | `-dataset <ds> [-query <q>] -facets <f1,f2> [-k 50]` | Compute categorical facet value counts (`SearchFacets`) |
| `search-analytics` | `-uri <uri>` | Retrieve top queries, zero-result queries, and average latency (`SearchAnalytics`) |
| `multimodal-ingest` | `-dataset <ds> -id <id> [-modality text\|image\|audio] [-text <t>] [-caption <c>] [-file <blob>]` | Ingest and embed text, PNG/JPEG image, or PCM16 WAV audio (`MultiModalIngest`) |
| `multimodal-search` | `-dataset <ds> [-modality text\|image\|audio] [-text <q>] [-vector f1,f2...] [-k 10]` | Cross-modal similarity search (`MultiModalSearch`) |
| `federated-search` | `-collections <ds1,ds2> [-tags t1,t2] [-vector f1,f2...] [-k 10] [-rrf-k 60]` | Parallel cross-collection k-NN with Reciprocal Rank Fusion (`FederatedSearch`) |
| `geo-search` | `-dataset <ds> -lat <deg> -lon <deg> -radius <km> [-k 10]` | Geospatial Haversine radius search |
| `recommend` | `-dataset <ds> -seeds <id1,id2> [-k 10] [-alpha 0.5] [-hops 2] [-decay 0.5]` | Hybrid vector + multi-hop graph recommendation |
| `temporal-search` | `-dataset <ds> -type <as_of\|range\|window> [-ts <ns>] [-start <ns>] [-end <ns>] [-window 10] [-k 10]` | Time-travel and sliding-window search |

### Reranking, Query Rewriting, Explainability & Provenance

| Command | Flags | Description |
| :--- | :--- | :--- |
| `rerank` | `-query <q> -candidates <json> [-strategy heuristic\|cross_encoder\|mmr\|hybrid] [-top-k 10]` | Re-score candidate documents (`Rerank`) |
| `eval-reranker` | `-query <q> -candidates <json> -relevant <id1,id2> [-strategy cross_encoder] [-k 10]` | Compute NDCG@K, MRR@K, and Recall@K (`EvaluateReranker`) |
| `rewrite-query` | `-query <q> [-strategy synonyms\|multi_query\|hyde]` | Expand queries with synonyms, sub-queries, or HyDE hypotheses (`RewriteQuery`) |
| `explain-search` | `-dataset <ds> [-vector f1,f2...] [-text <q>] [-alpha 0.5] [-k 5]` | Detailed dense/sparse/RRF/graph score breakdown and HNSW hop trace (`ExplainSearch`) |
| `explain-plan` | `-dataset <ds> [-k 10] [-ef 64] [-selectivity 1.0]` | Cost-based physical execution plan (`ExplainQueryPlan`) |
| `record-provenance` | `-dataset <ds> -id <vec-id> [-source <doc>] [-model <m>] [-version v1] [-chunk 0]` | Attach lineage metadata to a vector (`RecordProvenance`) |
| `get-provenance` | `-dataset <ds> -id <vec-id>` | Retrieve lineage and model provenance for a vector (`GetProvenance`) |

```bash
# Advanced full-text search with fuzzy matching and facets
longbow-cli text-search -dataset docs -query 'distributed AND "vector"~1' -facets category,author -k 10

# Explain search score attribution and HNSW hop trace
longbow-cli explain-search -dataset docs -vector 0.1,0.2,0.3,0.4 -text "vector store" -alpha 0.6 -k 5
```

---

## 4. Graph & GraphRAG Commands

| Command | Flags | Description |
| :--- | :--- | :--- |
| `add-edge` | `-dataset <ds> -subject <id> -predicate <rel> -object <id> [-weight 1.0]` | Insert a directed knowledge graph triple (`add-edge`) |
| `traverse` | `-dataset <ds> -start <id> [-hops 2] [-incoming] [-weighted]` | Execute BFS traversal over explicit graph edges (`traverse-graph`) |
| `get-graph-stats` | `-dataset <ds>` | Return node, edge, and predicate statistics (`GetGraphStats`) |
| `pagerank` | `-dataset <ds> [-damping 0.85] [-iterations 20]` | Compute PageRank centrality over the graph |
| `detect-communities` | `-dataset <ds> [-iterations 10]` | Run Label Propagation community detection |

---

## 5. Security, RBAC, Row-Level Security & Audit Log

| Command | Flags | Description |
| :--- | :--- | :--- |
| `create-api-key` | `-name <n> [-role reader\|writer\|tenant-admin\|admin] [-tenant default] [-datasets d1,d2] [-ttl 0]` | Issue a new HMAC/SHA-256 API key (`CreateAPIKey`) |
| `rotate-api-key` | `-key <raw-key> [-grace 3600]` | Rotate an API key with an overlap grace period (`RotateAPIKey`) |
| `revoke-api-key` | `-key <raw-key-or-id>` | Immediately revoke an API key (`RevokeAPIKey`) |
| `list-api-keys` | `-uri <uri>` | List redacted API key metadata (`ListAPIKeys`) |
| `set-rls-policy` | `-tenant <t> -dataset <ds> -field <col> [-op eq\|neq\|in\|contains] -values <v1,v2>` | Configure a Row-Level Security metadata filter (`SetRLSPolicy`) |
| `audit-log` | `[-tenant <t>] [-limit 50]` | Fetch structured security and data-access audit log events (`GetAuditLog`) |

```bash
# Issue a tenant-scoped writer key and attach an RLS policy
longbow-cli create-api-key -name ingest-bot -role writer -tenant acme -datasets acme_docs -ttl 86400
longbow-cli set-rls-policy -tenant acme -dataset acme_docs -field department -op in -values engineering,research
longbow-cli audit-log -tenant acme -limit 25
```

---

## 6. Online Backups, Verification & Point-in-Time Recovery (PITR)

| Command | Flags | Description |
| :--- | :--- | :--- |
| `backup` | `[-dataset <ds>] [-dest <dir>]` | Trigger an online checksummed snapshot backup (`TriggerBackup`) |
| `list-backups` | `-uri <uri>` | List available backup manifests (`ListBackups`) |
| `verify-backup` | `-backup-id <id>` | Verify SHA-256 file checksums of a backup manifest (`VerifyBackup`) |
| `restore` | `-backup-id <id> [-dataset <ds>] [-pitr-ts <unix-ns>]` | Restore a backup and optionally replay WAL entries up to `-pitr-ts` (`RestoreBackup`) |

```bash
longbow-cli backup -dataset prod_vectors -dest /var/backups/longbow
longbow-cli list-backups
longbow-cli verify-backup -backup-id bkp-1763100000
longbow-cli restore -backup-id bkp-1763100000 -dataset prod_vectors -pitr-ts 1763100500000000000
```

---

## 7. Change Data Capture (CDC) & Streaming Connectors

| Command | Flags | Description |
| :--- | :--- | :--- |
| `cdc-status` | `[-dataset <ds>]` | Inspect CDC stream cursors, subscriber counts, and lag (`CDCStatus`) |
| `stream-create` | `-name <n> -dataset <ds> [-source kafka\|pulsar\|webhook] [-brokers <addr>] [-topic <t>] [-group <g>] [-dim 0] [-dlq <topic>] [-autostart]` | Create a streaming ingestion connector (`CreateStreamConnector`) |
| `stream-stop` | `-name <n>` | Stop a running streaming connector (`StopStreamConnector`) |
| `stream-list` | `-uri <uri>` | List all registered streaming connectors (`ListStreamConnectors`) |
| `stream-stats` | `-name <n>` | Inspect committed offsets, deduplicated count, and DLQ depth (`GetStreamConnectorStats`) |
| `stream-ingest` | `-name <n> -id <vec-id> -vector <f1,f2...> [-text <t>] [-op upsert\|delete] [-key <idemp-key>]` | Push a streaming vector event with idempotency tracking (`IngestStreamEvent`) |

---

## 8. Usage Metering, Tenant Quotas & Billing

| Command | Flags | Description |
| :--- | :--- | :--- |
| `usage` | `[-tenant <t>]` | Inspect per-tenant or all-tenant vector count, storage bytes, and QPS (`GetTenantUsage`) |
| `set-quota` | `-tenant <t> [-max-vectors 0] [-max-bytes 0] [-max-qps 0] [-max-ingest-rps 0] [-soft-ratio 0.8] [-hard-enforce]` | Set soft-warning and hard-rejection tenant resource quotas (`SetTenantQuota`) |
| `export-billing` | `[-format json\|csv]` | Export itemized usage and cost billing report (`ExportBillingReport`) |
| `admin-usage` | `-uri http://127.0.0.1:8080` | Fetch the unified cloud, tenant usage, and API traffic summary via `GET /api/v1/admin/usage` |

---

## 9. Elastic Sharding, Rebalancing & Auto-Scaling

| Command | Flags | Description |
| :--- | :--- | :--- |
| `shard-topology` | `-dataset <ds>` | View shard ranges, vector counts, and node assignments (`GetClusterTopology`) |
| `shard-split` | `-dataset <ds> -shard <shard-id>` | Split an overloaded shard into two balanced shards (`TriggerShardSplit`) |
| `shard-merge` | `-dataset <ds> -left <shard-a> -right <shard-b>` | Merge two adjacent low-utilization shards (`TriggerShardMerge`) |
| `shard-rebalance` | `-dataset <ds> -nodes <n1,n2,n3>` | Rebalance shard assignments across active cluster nodes (`TriggerRebalance`) |
| `autoscale-config` | `[-enabled] [-min-replicas 1] [-max-replicas 16] [-target-qps 1000] [-target-p99-ms 25]` | View or update auto-scaling policy thresholds (`GetAutoScaleConfig` / `SetAutoScaleConfig`) |

---

## 10. Schema Registry, Constraints & Live Migration

| Command | Flags | Description |
| :--- | :--- | :--- |
| `register-schema` | `-dataset <ds> [-mode strict\|backward\|additive] -fields <json-array>` | Register a versioned dataset schema and column constraints (`RegisterSchema`) |
| `get-schema` | `-dataset <ds>` | Inspect the active schema version and history (`GetSchemaRegistry`) |
| `validate-schema` | `-dataset <ds> -record <json-object>` | Validate a record payload against dataset constraints (`ValidateSchema`) |
| `migrate-schema` | `-dataset <ds> -target <version> -fields <json-array>` | Execute zero-downtime live schema evolution (`MigrateSchema`) |

```bash
longbow-cli register-schema -dataset users -mode additive \
  -fields '[{"name":"email","type":"string","required":true,"not_null":true}]'
longbow-cli validate-schema -dataset users -record '{"email":"dev@example.com"}'
```

---

## 11. Materialized Views & Streaming Aggregations

| Command | Flags | Description |
| :--- | :--- | :--- |
| `create-view` | `-name <view> -source <ds> [-group-by <col>] [-agg count\|sum\|avg\|min\|max] [-agg-field <col>] [-refresh streaming\|periodic\|manual]` | Create a pre-computed materialized view (`CreateMaterializedView`) |
| `refresh-view` | `-name <view>` | Manually recompute a materialized view (`RefreshMaterializedView`) |
| `query-view` | `-name <view> [-group-key <val>]` | Query aggregated rows from a materialized view (`QueryMaterializedView`) |
| `list-views` | `-uri <uri>` | List all materialized views (`ListMaterializedViews`) |

---

## 12. Managed Cloud Control Plane, REST v1 & GraphQL

| Command | Flags | Description |
| :--- | :--- | :--- |
| `cloud-create-cluster` | `-id <id> -tenant <t> [-region us-east-1] [-tier serverless\|dedicated\|enterprise] [-mode async\|semi_sync\|sync] [-replicas 2] [-dim 128]` | Provision a managed cluster via `POST /api/v1/cloud/clusters` |
| `cloud-list-clusters` | `[-tenant <t>]` | List managed clusters via `GET /api/v1/cloud/clusters` |
| `cloud-scale-cluster` | `-id <id> -replicas <n>` | Scale cluster replicas via `POST /api/v1/cloud/clusters/{id}/scale` |
| `cloud-replicate-region` | `-id <id> -region <r>` | Replicate a cluster to a peer region via `POST .../replicate` |
| `cloud-failover-region` | `-id <id> -region <r>` | Promote a replica region to primary via `POST .../failover` |
| `rest-list-datasets` | `-uri http://127.0.0.1:8080` | List datasets over HTTP REST (`GET /api/v1/datasets`) |
| `rest-search` | `-dataset <ds> [-vector f1,f2...] [-text <q>] [-alpha 1.0] [-k 10]` | Run vector/hybrid search over HTTP REST (`POST /api/v1/datasets/{name}/search`) |
| `graphql` | `-query <gql> [-vars <json>]` | Execute a GraphQL query or mutation (`POST /api/v1/graphql`) |
