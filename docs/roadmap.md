# Longbow Feature Parity Roadmap (Items 1–15)

This document defines the implementation plan for closing Longbow's feature gaps
against leading vector databases. Each item includes required subtasks.

---

## 1. Managed Cloud / SaaS Control Plane & Multi-Region Replication

### Item 1 Subtasks

- 1.1 Define managed service tiers (`serverless`, `dedicated`, `enterprise`) with SLA targets (availability, max lag ms, RPO, RTO, P99 latency).
- 1.2 Implement cluster lifecycle control plane (provisioning, scaling nodes/tiers, regional attachment, decommissioning).
- 1.3 Implement cross-region mutation replication with monotonic LSNs, SHA-256 event checksums, and `async`/`semi_sync`/`sync` modes.
- 1.4 Implement lowest-lag healthy read-region routing (`SelectReadRegion`) and automated/manual failover promotion (`PromoteRegion`).
- 1.5 Add continuous SLA compliance evaluation (`EvaluateSLA`) and Prometheus metrics (`longbow_cloud_*`).
- 1.6 Expose Cloud Control Plane over Flight `DoAction`, REST (`/api/v1/cloud/*`), GraphQL, CLI (`cloud-*`), and Python SDK (`cloud_*`).
- 1.7 Add multi-region HTTP replication, failover, checksum verification, and fuzz tests.

---

## 2. First-Class REST/HTTP v1 & GraphQL APIs

### Item 2 Subtasks

- 2.1 Implement versioned `/api/v1/*` REST API for health, namespaces, datasets, vector CRUD, dense search, hybrid search, recommendations, and aggregations.
- 2.2 Serve OpenAPI v3.0.3 specification at `GET /api/v1/openapi.json`.
- 2.3 Implement pure-Go GraphQL engine (`POST`/`GET /api/v1/graphql` and `GraphQLQuery` `DoAction`) supporting queries, mutations, variables, aliases, field projection, and `__schema`/`__type` introspection.
- 2.4 Protect HTTP REST/GraphQL/usage/cloud routes with RBAC HTTP authentication middleware (`security.HTTPAuthMiddleware`) and dataset/RLS enforcement.
- 2.5 Add end-to-end REST, GraphQL, RBAC HTTP 401/403 enforcement, and parser/payload fuzz tests.

---

## 3. Role-Based Access Control / API Keys / Tenant Isolation

### Item 3 Subtasks

- 3.1 Design RBAC model (roles: admin, writer, reader, tenant-admin).
- 3.2 Implement API key issuance, rotation, and revocation.
- 3.3 Add tenant isolation at dataset/namespace level (row-level security).
- 3.4 Enforce authorization in gRPC/Arrow Flight and HTTP handlers.
- 3.5 Add audit logging for all privileged operations.
- 3.6 Update Python SDK and CLI for API key auth.
- 3.7 Add tests for permission boundaries and tenant leakage.

---

## 4. Backup, Restore, and Point-in-Time Recovery

### Item 4 Subtasks

- 4.1 Define snapshot format for datasets, indexes, and metadata.
- 4.2 Implement online snapshot to local/S3/GCS storage.
- 4.3 Implement restore workflow with consistency checks.
- 4.4 Add point-in-time recovery using WAL replay.
- 4.5 Add backup scheduling and retention policies.
- 4.6 Add CLI/SDK commands for backup/restore.
- 4.7 Add tests for crash consistency and partial restore.

---

## 5. Real-Time CDC and Streaming Ingestion (Kafka/Pulsar Connectors)

### Item 5 Subtasks

- 5.1 Implement CDC event format and change log.
- 5.2 Add Kafka connector (consumer + producer) for vector upserts.
- 5.3 Add Pulsar connector with schema registry support.
- 5.4 Implement streaming ingestion pipeline with backpressure.
- 5.5 Add exactly-once semantics and idempotent upserts.
- 5.6 Add monitoring for lag and throughput.
- 5.7 Add integration tests with testcontainers.

---

## 6. Multi-Modal (Image/Audio) Embedding Pipelines

### Item 6 Subtasks

- 6.1 Add image embedding backend (CLIP/OpenCLIP) via ONNX/WASM.
- 6.2 Add audio embedding backend (Whisper/CLAP) via ONNX/WASM.
- 6.3 Implement multi-modal dataset schema (blob + vector).
- 6.4 Add ingestion pipeline for media files (S3/Parquet).
- 6.5 Add multi-modal search (text-to-image, image-to-text).
- 6.6 Update Python SDK for media inputs.
- 6.7 Add benchmarks for multi-modal throughput.

---

## 7. Advanced Reranking (Cross-Encoder, LLM-Based) and Query Rewriting

### Item 7 Subtasks

- 7.1 Standardize reranker interface (already MLBackend).
- 7.2 Add LLM-based reranker (OpenAI/Cohere/local).
- 7.3 Add query rewriting/expansion module.
- 7.4 Add hybrid reranking (ML + heuristic).
- 7.5 Add caching for rerank results.
- 7.6 Add evaluation harness for rerank quality.
- 7.7 Update docs and SDK examples.

---

## 8. Serverless/Edge SDKs and WASM Client

### Item 8 Subtasks

- 8.1 Build WASM client for browser (Arrow + gRPC-web).
- 8.2 Add edge runtime (Cloudflare Workers, Deno).
- 8.3 Implement offline-first sync and conflict resolution.
- 8.4 Add lightweight local index (HNSW) in WASM.
- 8.5 Add SDK packages (npm, pip).
- 8.6 Add examples and docs.
- 8.7 Add E2E tests in browser.

---

## 9. Cost/Usage Metering and Quotas

### Item 9 Subtasks

- 9.1 Define usage metrics (queries, vectors, storage, egress).
- 9.2 Implement per-tenant metering and aggregation.
- 9.3 Add quota enforcement (hard/soft limits).
- 9.4 Add billing export (Prometheus/OTLP).
- 9.5 Add admin dashboard for usage.
- 9.6 Add tests for quota edge cases.

---

## 10. Auto-Scaling and Elastic Sharding

### Item 10 Subtasks

- 10.1 Implement shard split/merge operations.
- 10.2 Add load-based auto-scaling controller.
- 10.3 Add rebalancing and data migration.
- 10.4 Add cluster membership and discovery.
- 10.5 Add health checks and failover.
- 10.6 Add performance tests for scaling.
- 10.7 Update deployment docs (Helm/K8s).

---

## 11. Data Encryption at Rest and In Transit (TLS)

### Item 11 Subtasks

- 11.1 Enable TLS/mTLS for gRPC and HTTP.
- 11.2 Add encryption at rest for disk spill and WAL.
- 11.3 Implement key management (KMS/Vault).
- 11.4 Add certificate rotation.
- 11.5 Add compliance docs (SOC2/GDPR).
- 11.6 Add tests for encrypted I/O.

---

## 12. Schema Validation and Constraints

### Item 12 Subtasks

- 12.1 Define schema registry and validation rules.
- 12.2 Add constraints (NOT NULL, UNIQUE, CHECK).
- 12.3 Implement schema evolution with compatibility checks.
- 12.4 Add metadata type enforcement.
- 12.5 Add migration tooling.
- 12.6 Add tests for schema violations.

---

## 13. Materialized Views / Aggregations

### Item 13 Subtasks

- 13.1 Define materialized view storage.
- 13.2 Implement aggregation engine (count, sum, avg, etc.).
- 13.3 Add refresh policies (manual/scheduled/streaming).
- 13.4 Add query planner integration.
- 13.5 Add consistency guarantees.
- 13.6 Add benchmarks for aggregation throughput.

---

## 14. Full-Text Search Advanced (Highlighting, Facets)

### Item 14 Subtasks

- 14.1 Add highlighting of matched terms.
- 14.2 Implement faceted search (counts per category).
- 14.3 Add fuzzy/phrase/boolean queries.
- 14.4 Add ranking tuning (BM25 params).
- 14.5 Add search analytics.
- 14.6 Add tests for highlight/facet accuracy.

---

## 15. Explainability / Vector Provenance

### Item 15 Subtasks

- 15.1 Add score breakdowns (dense/sparse/rrf).
- 15.2 Add provenance metadata (model, version, timestamp).
- 15.3 Add debugging endpoints for search paths.
- 15.4 Add visualization support in SDK.
- 15.5 Add docs and examples.
- 15.6 Add tests for provenance correctness.

---

## Implementation Reference (Items 1–15)

| Item | Feature Area | Core Packages / Files |
| :--- | :--- | :--- |
| 1 | Managed Cloud Control Plane & Multi-Region Replication | `internal/store/{cloud_control_plane.go,cloud_control_plane_test.go}`, `cmd/cli/cloud_api_cmd.go`, `longbowclientsdk/src/longbow/client.py` |
| 2 | First-Class REST/HTTP v1, OpenAPI 3.0 & GraphQL APIs | `internal/store/{rest_graphql.go,rest_graphql_test.go,features_hub.go}`, `longbowclientsdk/src/longbow/client.py` |
| 3 | RBAC, API Keys, Tenant Isolation, RLS & Audit | `internal/security/{tenant.go,audit.go}`, `internal/middleware/auth_interceptor.go`, `internal/store/{rbac.go,rbac_actions.go,rest_graphql.go}`, `cmd/cli/security_cmd.go` |
| 4 | Online Backup, Checksums & WAL PITR | `internal/store/{backup.go,backup_pitr.go,backup_test.go}`, `cmd/cli/security_cmd.go` |
| 5 | Real-Time CDC & Kafka/Pulsar Streaming | `internal/store/{streaming_connectors.go,streaming_connectors_test.go}` |
| 6 | Multi-Modal Image/Audio Pipelines | `internal/store/{multimodal.go,multimodal_test.go}`, `longbowclientsdk/src/longbow/client.py` |
| 7 | Advanced Reranking & Query Rewriting | `internal/store/{reranker_advanced.go,reranker_advanced_test.go}`, `longbowclientsdk/src/longbow/client.py` |
| 8 | Serverless/Edge SDKs & WASM Client | `pkg/wasmclient/`, `cmd/longbow-wasm/`, `internal/store/edge_http.go`, `sdk/edge-js/`, [edge-wasm.md](edge-wasm.md) |
| 9 | Cost/Usage Metering, Quotas & Admin Dashboard | `internal/store/{metering.go,metering_test.go,rest_graphql.go}`, `internal/metrics/feature_metrics.go`, `grafana/dashboards/usage-metering-billing.json` |
| 10 | Auto-Scaling & Elastic Sharding | `internal/store/{elastic_sharding.go,elastic_sharding_test.go}`, `helm/longbow/templates/hpa.yaml` |
| 11 | Encryption at Rest, gRPC+HTTP TLS/mTLS & KMS/Vault | `internal/security/{tls.go,kms.go}`, `internal/storage/encryption.go`, `cmd/longbow/main.go`, [security-compliance.md](security-compliance.md) |
| 12 | Schema Validation, Constraints & Evolution | `internal/store/{schema_constraints.go,items_12_15_test.go}` |
| 13 | Materialized Views & Aggregations | `internal/store/{materialized_views.go,items_12_15_test.go}` |
| 14 | Advanced Full-Text Search (Highlight, Facets, BM25, Analytics) | `internal/store/{text_search_advanced.go,items_12_15_test.go}` |
| 15 | Explainability, Score Breakdowns & Vector Provenance | `internal/store/{provenance_explain.go,items_12_15_test.go}`, `longbowclientsdk/src/longbow/client.py` |
