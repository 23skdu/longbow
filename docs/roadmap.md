# Longbow Feature Parity Roadmap (Items 1–15)

This document defines the completed implementation of Longbow's enterprise feature
parity items (Items 1–15) and BM25 indexing performance improvements. All subtasks
are implemented and verified across the Go server, Arrow Flight `DoAction`, REST
`/api/v1/*`, OpenAPI 3.0.3, GraphQL, CLI (`cmd/cli`), WASM/Edge client, and
Python SDK (`longbowclientsdk`).

---

## 1. Managed Cloud / SaaS Control Plane & Multi-Region Replication (Completed)

### Item 1 Subtasks

- [x] 1.1 Define managed service tiers (`serverless`, `dedicated`, `enterprise`) with SLA targets (availability, max lag ms, RPO, RTO, P99 latency).
- [x] 1.2 Implement cluster lifecycle control plane (provisioning, scaling nodes/tiers, regional attachment, decommissioning).
- [x] 1.3 Implement cross-region mutation replication with monotonic LSNs, SHA-256 event checksums, and `async`/`semi_sync`/`sync` modes.
- [x] 1.4 Implement lowest-lag healthy read-region routing (`SelectReadRegion`) and automated/manual failover promotion (`PromoteRegion`).
- [x] 1.5 Add continuous SLA compliance evaluation (`EvaluateSLA`) and Prometheus metrics (`longbow_cloud_*`).
- [x] 1.6 Expose Cloud Control Plane over Flight `DoAction`, REST (`/api/v1/cloud/*`), GraphQL, CLI (`cloud-*`), and Python SDK (`cloud_*`).
- [x] 1.7 Add multi-region HTTP replication, failover, checksum verification, and fuzz tests.

---

## 2. First-Class REST/HTTP v1 & GraphQL APIs (Completed)

### Item 2 Subtasks

- [x] 2.1 Implement versioned `/api/v1/*` REST API for health, namespaces, datasets, vector CRUD, dense search, hybrid search, recommendations, aggregations, RBAC, backups, streaming, reranking, sharding, materialized views, schema registry, and provenance.
- [x] 2.2 Serve OpenAPI v3.0.3 specification at `GET /api/v1/openapi.json`.
- [x] 2.3 Implement pure-Go GraphQL engine (`POST`/`GET /api/v1/graphql` and `GraphQLQuery` `DoAction`) supporting queries, mutations, variables, aliases, field projection, and `__schema`/`__type` introspection.
- [x] 2.4 Protect HTTP REST/GraphQL/usage/cloud routes with RBAC HTTP authentication middleware (`security.HTTPAuthMiddleware`) and dataset/RLS enforcement.
- [x] 2.5 Add end-to-end REST, GraphQL, RBAC HTTP 401/403 enforcement, and parser/payload fuzz tests.

---

## 3. Role-Based Access Control / API Keys / Tenant Isolation (Completed)

### Item 3 Subtasks

- [x] 3.1 Design RBAC model (roles: admin, writer, reader, tenant-admin).
- [x] 3.2 Implement API key issuance, rotation, and revocation.
- [x] 3.3 Add tenant isolation at dataset/namespace level (row-level security).
- [x] 3.4 Enforce authorization in gRPC/Arrow Flight and HTTP handlers.
- [x] 3.5 Add audit logging for all privileged operations.
- [x] 3.6 Update Python SDK (`create_api_key`, `rotate_api_key`, `revoke_api_key`, `list_api_keys`, `set_rls_policy`, `get_audit_log`) and CLI (`create-api-key`, `rotate-api-key`, `revoke-api-key`, `list-api-keys`, `set-rls-policy`, `audit-log`) for API key auth.
- [x] 3.7 Add tests for permission boundaries and tenant leakage.

---

## 4. Backup, Restore, and Point-in-Time Recovery (Completed)

### Item 4 Subtasks

- [x] 4.1 Define snapshot format for datasets, indexes, and metadata.
- [x] 4.2 Implement online snapshot to local/S3/GCS storage.
- [x] 4.3 Implement restore workflow with consistency checks.
- [x] 4.4 Add point-in-time recovery using WAL replay.
- [x] 4.5 Add backup scheduling and retention policies.
- [x] 4.6 Add CLI (`backup`, `restore`, `list-backups`, `verify-backup`), REST (`/api/v1/backups*`), GraphQL, and SDK (`trigger_backup`, `restore_backup`, `list_backups`, `verify_backup`) commands for backup/restore.
- [x] 4.7 Add tests for crash consistency and partial restore.

---

## 5. Real-Time CDC and Streaming Ingestion (Kafka/Pulsar Connectors) (Completed)

### Item 5 Subtasks

- [x] 5.1 Implement CDC event format and change log.
- [x] 5.2 Add Kafka connector (consumer + producer) for vector upserts.
- [x] 5.3 Add Pulsar connector with schema registry support.
- [x] 5.4 Implement streaming ingestion pipeline with backpressure.
- [x] 5.5 Add exactly-once semantics and idempotent upserts.
- [x] 5.6 Add monitoring for lag and throughput.
- [x] 5.7 Add integration tests with TCP Kafka broker / testcontainers (`TestKafkaBrokerAndTestcontainersConnectorIntegration`).

---

## 6. Multi-Modal (Image/Audio) Embedding Pipelines (Completed)

### Item 6 Subtasks

- [x] 6.1 Add image embedding backend (CLIP/OpenCLIP) via ONNX/WASM.
- [x] 6.2 Add audio embedding backend (Whisper/CLAP) via ONNX/WASM.
- [x] 6.3 Implement multi-modal dataset schema (blob + vector).
- [x] 6.4 Add ingestion pipeline for media files (S3/Parquet).
- [x] 6.5 Add multi-modal search (text-to-image, image-to-text).
- [x] 6.6 Update Python SDK (`ingest_media`, `ingest_media_parquet`, `multimodal_search`) and CLI (`multimodal-ingest`, `multimodal-search`) for media inputs.
- [x] 6.7 Add benchmarks for multi-modal throughput.

---

## 7. Advanced Reranking (Cross-Encoder, LLM-Based) and Query Rewriting (Completed)

### Item 7 Subtasks

- [x] 7.1 Standardize reranker interface (already MLBackend).
- [x] 7.2 Add LLM-based reranker (OpenAI/Cohere/local).
- [x] 7.3 Add query rewriting/expansion module.
- [x] 7.4 Add hybrid reranking (ML + heuristic).
- [x] 7.5 Add caching for rerank results.
- [x] 7.6 Add evaluation harness (`EvaluateReranker` Flight action, REST `/api/v1/rerank/evaluate`, CLI `eval-reranker`, SDK `evaluate_reranker`) for rerank quality.
- [x] 7.7 Update docs and SDK examples.

---

## 8. Serverless/Edge SDKs and WASM Client (Completed)

### Item 8 Subtasks

- [x] 8.1 Build WASM client for browser (Arrow columnar `"LBAR"` + gRPC-Web 5-byte frame codec in `pkg/wasmclient/transport.go` and `EdgeSearchHTTPHandler`).
- [x] 8.2 Add edge runtime (Cloudflare Workers, Deno).
- [x] 8.3 Implement offline-first sync and conflict resolution.
- [x] 8.4 Add lightweight local index (HNSW) in WASM.
- [x] 8.5 Add SDK packages (npm, pip).
- [x] 8.6 Add examples and docs.
- [x] 8.7 Add E2E tests for browser/WASM gRPC-Web + Arrow transport (`pkg/wasmclient/edge_client_test.go`).

---

## 9. Cost/Usage Metering and Quotas (Completed)

### Item 9 Subtasks

- [x] 9.1 Define usage metrics (queries, vectors, storage, egress).
- [x] 9.2 Implement per-tenant metering and aggregation.
- [x] 9.3 Add quota enforcement (hard/soft limits).
- [x] 9.4 Add billing export (Prometheus/OTLP/JSON/CSV/Webhook).
- [x] 9.5 Add admin dashboard for usage.
- [x] 9.6 Add tests for quota edge cases.

---

## 10. Auto-Scaling and Elastic Sharding (Completed)

### Item 10 Subtasks

- [x] 10.1 Implement shard split/merge operations.
- [x] 10.2 Add load-based auto-scaling controller.
- [x] 10.3 Add rebalancing and data migration.
- [x] 10.4 Add cluster membership and discovery.
- [x] 10.5 Add health checks and failover.
- [x] 10.6 Add performance tests for scaling.
- [x] 10.7 Update deployment docs (Helm/K8s).

---

## 11. Data Encryption at Rest and In Transit (TLS) (Completed)

### Item 11 Subtasks

- [x] 11.1 Enable TLS/mTLS for gRPC and HTTP.
- [x] 11.2 Add encryption at rest for disk spill and WAL.
- [x] 11.3 Implement key management (KMS/Vault).
- [x] 11.4 Add certificate rotation.
- [x] 11.5 Add compliance docs (SOC2/GDPR).
- [x] 11.6 Add tests for encrypted I/O.

---

## 12. Schema Validation and Constraints (Completed)

### Item 12 Subtasks

- [x] 12.1 Define schema registry and validation rules.
- [x] 12.2 Add constraints (NOT NULL, UNIQUE, CHECK).
- [x] 12.3 Implement schema evolution with compatibility checks.
- [x] 12.4 Add metadata type enforcement (`StoreRecordBatch` + `DoPut`).
- [x] 12.5 Add migration tooling (`MigrateSchema` Flight action, REST `/schema/migrate`, CLI `migrate-schema`, SDK `migrate_schema`).
- [x] 12.6 Add tests for schema violations.

---

## 13. Materialized Views / Aggregations (Completed)

### Item 13 Subtasks

- [x] 13.1 Define materialized view storage.
- [x] 13.2 Implement aggregation engine (count, sum, avg, min, max).
- [x] 13.3 Add refresh policies (manual/scheduled/streaming wired into `applyBatchToMemory`).
- [x] 13.4 Add query planner integration (`TryPlanQuery` wired into `runDatasetAggregation`).
- [x] 13.5 Add consistency guarantees.
- [x] 13.6 Add benchmarks for aggregation throughput.

---

## 14. Full-Text Search Advanced (Highlighting, Facets) (Completed)

### Item 14 Subtasks

- [x] 14.1 Add highlighting of matched terms.
- [x] 14.2 Implement faceted search (counts per category).
- [x] 14.3 Add fuzzy/phrase/boolean queries.
- [x] 14.4 Add ranking tuning (BM25 params).
- [x] 14.5 Add search analytics.
- [x] 14.6 Add tests for highlight/facet accuracy.

---

## 15. Explainability / Vector Provenance (Completed)

### Item 15 Subtasks

- [x] 15.1 Add score breakdowns (dense/sparse/rrf).
- [x] 15.2 Add provenance metadata (model, version, timestamp).
- [x] 15.3 Add debugging endpoints for search paths (`ExplainSearch`, `DebugSearchPath`, REST `/explain`).
- [x] 15.4 Add visualization support in SDK (`format_search_explanation` and HTML/SVG `plot_search_explanation`).
- [x] 15.5 Add docs and examples.
- [x] 15.6 Add tests for provenance correctness.

---

## Implementation Reference (Items 1–15 & BM25 Arena/WAND Optimizations)

| Item | Feature Area | Core Packages / Files |
| :--- | :--- | :--- |
| 1 | Managed Cloud Control Plane & Multi-Region Replication | `internal/store/{cloud_control_plane.go,cloud_control_plane_test.go}`, `cmd/cli/cloud_api_cmd.go`, `longbowclientsdk/src/longbow/client.py` |
| 2 | First-Class REST/HTTP v1, OpenAPI 3.0 & GraphQL APIs | `internal/store/{rest_graphql.go,rest_graphql_test.go,features_hub.go}`, `longbowclientsdk/src/longbow/client.py` |
| 3 | RBAC, API Keys, Tenant Isolation, RLS & Audit | `internal/security/{tenant.go,audit.go}`, `internal/middleware/auth_interceptor.go`, `internal/store/{rbac.go,rbac_actions.go,rest_graphql.go}`, `cmd/cli/{security_cmd.go,cloud_api_cmd.go}` |
| 4 | Online Backup, Checksums & WAL PITR | `internal/store/{backup.go,backup_pitr.go,backup_test.go}`, `cmd/cli/{security_cmd.go,cloud_api_cmd.go}` |
| 5 | Real-Time CDC & Kafka/Pulsar Streaming | `internal/store/{streaming_connectors.go,streaming_connectors_test.go}`, `cmd/cli/cloud_api_cmd.go` |
| 6 | Multi-Modal Image/Audio Pipelines | `internal/store/{multimodal.go,multimodal_test.go}`, `cmd/cli/cloud_api_cmd.go`, `longbowclientsdk/src/longbow/client.py` |
| 7 | Advanced Reranking & Query Rewriting | `internal/store/{reranker_advanced.go,reranker_advanced_test.go}`, `cmd/cli/cloud_api_cmd.go`, `longbowclientsdk/src/longbow/client.py` |
| 8 | Serverless/Edge SDKs & WASM Client | `pkg/wasmclient/{edge_client.go,transport.go,edge_client_test.go}`, `cmd/longbow-wasm/`, `internal/store/edge_http.go`, `sdk/edge-js/`, [edge-wasm.md](edge-wasm.md) |
| 9 | Cost/Usage Metering, Quotas & Admin Dashboard | `internal/store/{metering.go,metering_test.go,rest_graphql.go}`, `internal/metrics/feature_metrics.go`, `grafana/dashboards/usage-metering-billing.json` |
| 10 | Auto-Scaling & Elastic Sharding | `internal/store/{elastic_sharding.go,elastic_sharding_test.go}`, `cmd/cli/cloud_api_cmd.go`, `helm/longbow/templates/hpa.yaml` |
| 11 | Encryption at Rest, gRPC+HTTP TLS/mTLS & KMS/Vault | `internal/security/{tls.go,kms.go}`, `internal/storage/encryption.go`, `cmd/longbow/main.go`, [security-compliance.md](security-compliance.md) |
| 12 | Schema Validation, Constraints & Evolution | `internal/store/{schema_constraints.go,store_ingest.go,items_12_15_test.go}`, `cmd/cli/cloud_api_cmd.go` |
| 13 | Materialized Views & Aggregations | `internal/store/{materialized_views.go,store_ingest.go,rest_graphql.go,items_12_15_test.go}`, `cmd/cli/cloud_api_cmd.go` |
| 14 | Advanced Full-Text Search (Highlight, Facets, BM25, Analytics) | `internal/store/{text_search_advanced.go,items_12_15_test.go}`, `internal/store/index/{bm25_arena.go,bm25_inverted_index.go}`, `cmd/cli/cloud_api_cmd.go` |
| 15 | Explainability, Score Breakdowns & Vector Provenance | `internal/store/{provenance_explain.go,items_12_15_test.go}`, `cmd/cli/cloud_api_cmd.go`, `longbowclientsdk/src/longbow/client.py` |
