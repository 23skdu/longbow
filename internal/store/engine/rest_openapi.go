package engine

import (
	"encoding/json"
	"net/http"

	"github.com/23skdu/longbow/internal/metrics"
)

// OpenAPIHandler serves the OpenAPI v3.0.3 JSON specification for Longbow HTTP v1 APIs.
func (s *VectorStore) OpenAPIHandler() http.Handler {
	spec := buildOpenAPISpec()
	raw, _ := json.MarshalIndent(spec, "", "  ")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "openapi", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
		metrics.RESTRequestsTotal.WithLabelValues("openapi", r.Method, "200").Inc()
	})
}

func buildOpenAPISpec() map[string]any {
	okResp := func(desc string) map[string]any {
		return map[string]any{
			"200": map[string]any{"description": desc},
			"401": map[string]any{"description": "Unauthorized"},
			"403": map[string]any{"description": "Forbidden"},
		}
	}
	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "Longbow Vector Database REST & GraphQL API",
			"version":     "1.0.0",
			"description": "First-class HTTP REST v1, GraphQL, Usage Metering Admin, and Managed Cloud Control Plane APIs.",
		},
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"bearerAuth": map[string]any{"type": "http", "scheme": "bearer"},
				"apiKeyAuth": map[string]any{"type": "apiKey", "in": "header", "name": "X-API-Key"},
			},
		},
		"paths": map[string]any{
			"/api/v1/health":                            map[string]any{"get": map[string]any{"summary": "Health check", "responses": okResp("Healthy")}},
			"/api/v1/openapi.json":                      map[string]any{"get": map[string]any{"summary": "OpenAPI schema", "responses": okResp("OpenAPI JSON")}},
			"/api/v1/graphql":                           map[string]any{"post": map[string]any{"summary": "Execute GraphQL query/mutation", "responses": okResp("GraphQL result")}},
			"/api/v1/namespaces":                        map[string]any{"get": map[string]any{"summary": "List namespaces", "responses": okResp("Namespaces")}, "post": map[string]any{"summary": "Create namespace", "responses": okResp("Created")}},
			"/api/v1/namespaces/{name}":                 map[string]any{"delete": map[string]any{"summary": "Delete namespace", "responses": okResp("Deleted")}},
			"/api/v1/datasets":                          map[string]any{"get": map[string]any{"summary": "List datasets", "responses": okResp("Datasets")}, "post": map[string]any{"summary": "Create/seed dataset", "responses": okResp("Created")}},
			"/api/v1/datasets/{name}":                   map[string]any{"get": map[string]any{"summary": "Inspect dataset", "responses": okResp("Dataset info")}, "delete": map[string]any{"summary": "Drop dataset", "responses": okResp("Dropped")}},
			"/api/v1/datasets/{name}/vectors":           map[string]any{"post": map[string]any{"summary": "Upsert JSON vectors", "responses": okResp("Upserted")}},
			"/api/v1/datasets/{name}/vectors/{id}":      map[string]any{"get": map[string]any{"summary": "Get vector by ID", "responses": okResp("Vector")}, "delete": map[string]any{"summary": "Delete vector by ID", "responses": okResp("Deleted")}},
			"/api/v1/datasets/{name}/search":            map[string]any{"post": map[string]any{"summary": "Vector search (dense + filtered)", "responses": okResp("Search results")}},
			"/api/v1/datasets/{name}/search/by-id":      map[string]any{"post": map[string]any{"summary": "Vector search by stored vector ID", "responses": okResp("Search results")}},
			"/api/v1/datasets/{name}/hybrid":            map[string]any{"post": map[string]any{"summary": "Hybrid vector + BM25 + graph search", "responses": okResp("Hybrid results")}},
			"/api/v1/datasets/{name}/text-search":       map[string]any{"post": map[string]any{"summary": "Advanced full-text search (Boolean AST, phrase, fuzzy, snippets, facets)", "responses": okResp("Full-text results")}},
			"/api/v1/datasets/{name}/geo-search":        map[string]any{"post": map[string]any{"summary": "Geospatial radius, bounding-box, or geo+vector hybrid search", "responses": okResp("Geospatial results")}},
			"/api/v1/datasets/{name}/temporal-search":   map[string]any{"post": map[string]any{"summary": "Temporal as-of, range, or sliding-window search", "responses": okResp("Temporal results")}},
			"/api/v1/datasets/{name}/multimodal-search": map[string]any{"post": map[string]any{"summary": "Cross-modal text/image/audio search", "responses": okResp("Multi-modal results")}},
			"/api/v1/datasets/{name}/multimodal-ingest": map[string]any{"post": map[string]any{"summary": "Multi-modal item ingestion", "responses": okResp("Ingested")}},
			"/api/v1/datasets/{name}/range-search":      map[string]any{"post": map[string]any{"summary": "Distance-threshold range search", "responses": okResp("Range results")}},
			"/api/v1/datasets/{name}/recommend":         map[string]any{"post": map[string]any{"summary": "Seed recommendations", "responses": okResp("Recommended results")}},
			"/api/v1/datasets/{name}/aggregations":      map[string]any{"post": map[string]any{"summary": "Columnar & metadata aggregations", "responses": okResp("Aggregation rows")}},
			"/api/v1/datasets/{name}/bm25":              map[string]any{"post": map[string]any{"summary": "Tune BM25 k1/b parameters", "responses": okResp("Tuned")}},
			"/api/v1/datasets/{name}/facets":            map[string]any{"post": map[string]any{"summary": "Compute search facet counts", "responses": okResp("Facets")}},
			"/api/v1/datasets/{name}/explain":           map[string]any{"post": map[string]any{"summary": "Explain vector/hybrid search scoring & HNSW path", "responses": okResp("Explanation")}},
			"/api/v1/datasets/{name}/provenance":        map[string]any{"get": map[string]any{"summary": "Get vector provenance lineage", "responses": okResp("Provenance")}, "post": map[string]any{"summary": "Record vector provenance", "responses": okResp("Recorded")}},
			"/api/v1/datasets/{name}/schema":            map[string]any{"get": map[string]any{"summary": "Get registered dataset schema", "responses": okResp("Schema")}, "post": map[string]any{"summary": "Register dataset schema constraints", "responses": okResp("Registered")}},
			"/api/v1/datasets/{name}/schema/migrate":    map[string]any{"post": map[string]any{"summary": "Migrate dataset schema & backfill columns", "responses": okResp("Migrated")}},
			"/api/v1/search/federated":                  map[string]any{"post": map[string]any{"summary": "Federated cross-collection vector search (RRF)", "responses": okResp("Federated results")}},
			"/api/v1/search/analytics":                  map[string]any{"get": map[string]any{"summary": "Search query analytics & zero-hit telemetry", "responses": okResp("Analytics")}},
			"/api/v1/federated/collections":             map[string]any{"get": map[string]any{"summary": "List federated collections", "responses": okResp("Federated collections")}, "post": map[string]any{"summary": "Register collection tags/metadata for federated routing", "responses": okResp("Registered")}},
			"/api/v1/rbac/keys":                         map[string]any{"get": map[string]any{"summary": "List API keys", "responses": okResp("Keys")}, "post": map[string]any{"summary": "Create API key", "responses": okResp("Created")}},
			"/api/v1/rbac/rls":                          map[string]any{"post": map[string]any{"summary": "Register row-level security policy", "responses": okResp("Policy set")}},
			"/api/v1/rbac/audit":                        map[string]any{"get": map[string]any{"summary": "Fetch security audit log", "responses": okResp("Audit events")}},
			"/api/v1/oidc/providers":                    map[string]any{"get": map[string]any{"summary": "List configured OIDC federation providers", "responses": okResp("Providers")}, "post": map[string]any{"summary": "Register or update an OIDC federation provider", "responses": okResp("Provider registered")}, "delete": map[string]any{"summary": "Remove an OIDC federation provider", "responses": okResp("Provider removed")}},
			"/api/v1/oidc/token":                        map[string]any{"post": map[string]any{"summary": "Exchange federated OIDC JWT for short-lived Longbow session token", "responses": okResp("Session token")}},
			"/api/v1/backups":                           map[string]any{"get": map[string]any{"summary": "List online backups", "responses": okResp("Backups")}, "post": map[string]any{"summary": "Trigger online backup", "responses": okResp("Backup metadata")}},
			"/api/v1/backups/restore":                   map[string]any{"post": map[string]any{"summary": "Restore backup with PITR", "responses": okResp("Restore report")}},
			"/api/v1/backups/verify":                    map[string]any{"post": map[string]any{"summary": "Verify backup integrity", "responses": okResp("Verification report")}},
			"/api/v1/streams/connectors":                map[string]any{"get": map[string]any{"summary": "List streaming CDC connectors", "responses": okResp("Connectors")}, "post": map[string]any{"summary": "Create streaming connector", "responses": okResp("Connector stats")}, "delete": map[string]any{"summary": "Stop streaming connector", "responses": okResp("Stopped")}},
			"/api/v1/streams/ingest":                    map[string]any{"post": map[string]any{"summary": "Ingest streaming envelopes", "responses": okResp("Ingest stats")}},
			"/api/v1/streams/schemas":                   map[string]any{"post": map[string]any{"summary": "Register stream schema", "responses": okResp("Registered schema")}},
			"/api/v1/rerank":                            map[string]any{"post": map[string]any{"summary": "Cross-encoder / ColBERT / LLM / MMR reranking", "responses": okResp("Reranked candidates")}},
			"/api/v1/rerank/evaluate":                   map[string]any{"post": map[string]any{"summary": "Evaluate reranker NDCG@10 / MRR@10 / Recall@10", "responses": okResp("Evaluation report")}},
			"/api/v1/rewrite-query":                     map[string]any{"post": map[string]any{"summary": "HyDE / Synonym / Step-Back query expansion", "responses": okResp("Expanded query")}},
			"/api/v1/sharding/topology":                 map[string]any{"get": map[string]any{"summary": "Get elastic sharding ring topology", "responses": okResp("Topology")}},
			"/api/v1/sharding/autoscale":                map[string]any{"get": map[string]any{"summary": "Get autoscale policy", "responses": okResp("Policy")}, "post": map[string]any{"summary": "Set autoscale policy", "responses": okResp("Updated")}},
			"/api/v1/sharding/split":                    map[string]any{"post": map[string]any{"summary": "Split hot shard", "responses": okResp("Split result")}},
			"/api/v1/sharding/merge":                    map[string]any{"post": map[string]any{"summary": "Merge adjacent shards", "responses": okResp("Merge result")}},
			"/api/v1/sharding/rebalance":                map[string]any{"post": map[string]any{"summary": "Rebalance dataset shards", "responses": okResp("Rebalance tasks")}},
			"/api/v1/views":                             map[string]any{"get": map[string]any{"summary": "List materialized views", "responses": okResp("Views")}, "post": map[string]any{"summary": "Create materialized view", "responses": okResp("View snapshot")}},
			"/api/v1/views/query":                       map[string]any{"post": map[string]any{"summary": "Query materialized view", "responses": okResp("View rows")}},
			"/api/v1/views/refresh":                     map[string]any{"post": map[string]any{"summary": "Refresh materialized view", "responses": okResp("Refreshed snapshot")}},
			"/api/v1/admin/usage":                       map[string]any{"get": map[string]any{"summary": "Tenant metering & billing summary", "responses": okResp("Usage summary")}},
			"/api/v1/cloud/clusters":                    map[string]any{"get": map[string]any{"summary": "List managed cloud clusters", "responses": okResp("Clusters")}, "post": map[string]any{"summary": "Create managed cloud cluster", "responses": okResp("Cluster created")}},
			"/api/v1/cloud/clusters/{id}/scale":         map[string]any{"post": map[string]any{"summary": "Scale cloud cluster", "responses": okResp("Scaled cluster")}},
			"/api/v1/cloud/clusters/{id}/regions":       map[string]any{"post": map[string]any{"summary": "Add replica region", "responses": okResp("Updated cluster")}},
			"/api/v1/cloud/clusters/{id}/promote":       map[string]any{"post": map[string]any{"summary": "Failover / promote region", "responses": okResp("Promoted cluster")}},
			"/api/v1/cloud/clusters/{id}/sla":           map[string]any{"get": map[string]any{"summary": "Evaluate cluster SLA compliance", "responses": okResp("SLA report")}},
		},
	}
}
