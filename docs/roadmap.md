# Longbow Feature Parity Roadmap (Items 3–15)

This document defines the implementation plan for closing Longbow's feature gaps
against leading vector databases. Each item includes required subtasks.

---

## 3. Role-Based Access Control / API Keys / Tenant Isolation

### Subtasks
- 3.1 Design RBAC model (roles: admin, writer, reader, tenant-admin).
- 3.2 Implement API key issuance, rotation, and revocation.
- 3.3 Add tenant isolation at dataset/namespace level (row-level security).
- 3.4 Enforce authorization in gRPC/Arrow Flight and HTTP handlers.
- 3.5 Add audit logging for all privileged operations.
- 3.6 Update Python SDK and CLI for API key auth.
- 3.7 Add tests for permission boundaries and tenant leakage.

---

## 4. Backup, Restore, and Point-in-Time Recovery

### Subtasks
- 4.1 Define snapshot format for datasets, indexes, and metadata.
- 4.2 Implement online snapshot to local/S3/GCS storage.
- 4.3 Implement restore workflow with consistency checks.
- 4.4 Add point-in-time recovery using WAL replay.
- 4.5 Add backup scheduling and retention policies.
- 4.6 Add CLI/SDK commands for backup/restore.
- 4.7 Add tests for crash consistency and partial restore.

---

## 5. Real-Time CDC and Streaming Ingestion (Kafka/Pulsar Connectors)

### Subtasks
- 5.1 Implement CDC event format and change log.
- 5.2 Add Kafka connector (consumer + producer) for vector upserts.
- 5.3 Add Pulsar connector with schema registry support.
- 5.4 Implement streaming ingestion pipeline with backpressure.
- 5.5 Add exactly-once semantics and idempotent upserts.
- 5.6 Add monitoring for lag and throughput.
- 5.7 Add integration tests with testcontainers.

---

## 6. Multi-Modal (Image/Audio) Embedding Pipelines

### Subtasks
- 6.1 Add image embedding backend (CLIP/OpenCLIP) via ONNX/WASM.
- 6.2 Add audio embedding backend (Whisper/CLAP) via ONNX/WASM.
- 6.3 Implement multi-modal dataset schema (blob + vector).
- 6.4 Add ingestion pipeline for media files (S3/Parquet).
- 6.5 Add multi-modal search (text-to-image, image-to-text).
- 6.6 Update Python SDK for media inputs.
- 6.7 Add benchmarks for multi-modal throughput.

---

## 7. Advanced Reranking (Cross-Encoder, LLM-Based) and Query Rewriting

### Subtasks
- 7.1 Standardize reranker interface (already MLBackend).
- 7.2 Add LLM-based reranker (OpenAI/Cohere/local).
- 7.3 Add query rewriting/expansion module.
- 7.4 Add hybrid reranking (ML + heuristic).
- 7.5 Add caching for rerank results.
- 7.6 Add evaluation harness for rerank quality.
- 7.7 Update docs and SDK examples.

---

## 8. Serverless/Edge SDKs and WASM Client

### Subtasks
- 8.1 Build WASM client for browser (Arrow + gRPC-web).
- 8.2 Add edge runtime (Cloudflare Workers, Deno).
- 8.3 Implement offline-first sync and conflict resolution.
- 8.4 Add lightweight local index (HNSW) in WASM.
- 8.5 Add SDK packages (npm, pip).
- 8.6 Add examples and docs.
- 8.7 Add E2E tests in browser.

---

## 9. Cost/Usage Metering and Quotas

### Subtasks
- 9.1 Define usage metrics (queries, vectors, storage, egress).
- 9.2 Implement per-tenant metering and aggregation.
- 9.3 Add quota enforcement (hard/soft limits).
- 9.4 Add billing export (Prometheus/OTLP).
- 9.5 Add admin dashboard for usage.
- 9.6 Add tests for quota edge cases.

---

## 10. Auto-Scaling and Elastic Sharding

### Subtasks
- 10.1 Implement shard split/merge operations.
- 10.2 Add load-based auto-scaling controller.
- 10.3 Add rebalancing and data migration.
- 10.4 Add cluster membership and discovery.
- 10.5 Add health checks and failover.
- 10.6 Add performance tests for scaling.
- 10.7 Update deployment docs (Helm/K8s).

---

## 11. Data Encryption at Rest and In Transit (TLS)

### Subtasks
- 11.1 Enable TLS/mTLS for gRPC and HTTP.
- 11.2 Add encryption at rest for disk spill and WAL.
- 11.3 Implement key management (KMS/Vault).
- 11.4 Add certificate rotation.
- 11.5 Add compliance docs (SOC2/GDPR).
- 11.6 Add tests for encrypted I/O.

---

## 12. Schema Validation and Constraints

### Subtasks
- 12.1 Define schema registry and validation rules.
- 12.2 Add constraints (NOT NULL, UNIQUE, CHECK).
- 12.3 Implement schema evolution with compatibility checks.
- 12.4 Add metadata type enforcement.
- 12.5 Add migration tooling.
- 12.6 Add tests for schema violations.

---

## 13. Materialized Views / Aggregations

### Subtasks
- 13.1 Define materialized view storage.
- 13.2 Implement aggregation engine (count, sum, avg, etc.).
- 13.3 Add refresh policies (manual/scheduled/streaming).
- 13.4 Add query planner integration.
- 13.5 Add consistency guarantees.
- 13.6 Add benchmarks for aggregation throughput.

---

## 14. Full-Text Search Advanced (Highlighting, Facets)

### Subtasks
- 14.1 Add highlighting of matched terms.
- 14.2 Implement faceted search (counts per category).
- 14.3 Add fuzzy/phrase/boolean queries.
- 14.4 Add ranking tuning (BM25 params).
- 14.5 Add search analytics.
- 14.6 Add tests for highlight/facet accuracy.

---

## 15. Explainability / Vector Provenance

### Subtasks
- 15.1 Add score breakdowns (dense/sparse/rrf).
- 15.2 Add provenance metadata (model, version, timestamp).
- 15.3 Add debugging endpoints for search paths.
- 15.4 Add visualization support in SDK.
- 15.5 Add docs and examples.
- 15.6 Add tests for provenance correctness.
EOF
echo done