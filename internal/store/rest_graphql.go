package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/store/types"
)

const maxRESTBodyBytes = 32 << 20

// httpAuthWrap applies HTTP RBAC authentication dynamically at request time whenever
// VectorStore RBAC is enabled (including when EnableRBAC is called after route registration).
func (s *VectorStore) httpAuthWrap(requiredPerm Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rbac := s.RBAC()
		if rbac == nil {
			next.ServeHTTP(w, r)
			return
		}
		security.HTTPAuthMiddleware(rbac, s.AuditLogger(), string(requiredPerm))(next).ServeHTTP(w, r)
	})
}

func (s *VectorStore) httpMethodAuthWrap(readPerm, writePerm Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		perm := readPerm
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			perm = writePerm
		}
		s.httpAuthWrap(perm, next).ServeHTTP(w, r)
	})
}

func (s *VectorStore) requireHTTPGlobalPermission(ctx context.Context, perm Permission, datasetPath string) error {
	if s.RBAC() == nil {
		return nil
	}
	if datasetPath != "" {
		_, err := s.AuthorizeDatasetAccess(ctx, perm, datasetPath)
		return err
	}
	tc, ok := security.TenantFromContext(ctx)
	if !ok {
		return security.ErrUnauthorized
	}
	if tc.IsAdmin {
		return nil
	}
	for _, p := range tc.Permissions {
		if p == string(perm) || p == string(PermAdmin) {
			return nil
		}
	}
	return security.ErrForbidden
}

func writeRESTJSON(w http.ResponseWriter, endpoint, method string, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
	metrics.RESTRequestsTotal.WithLabelValues(endpoint, method, strconv.Itoa(statusCode)).Inc()
}

func writeRESTError(w http.ResponseWriter, endpoint, method string, statusCode int, msg string) {
	writeRESTJSON(w, endpoint, method, statusCode, map[string]any{"error": msg})
}

func isValidResourceName(name string) bool {
	return name != "" && !strings.Contains(name, "..") && !strings.HasPrefix(name, "/")
}

// UpsertStreamEvents encodes JSON/struct vector events into an Arrow RecordBatch and waits for indexing.
func (s *VectorStore) UpsertStreamEvents(ctx context.Context, datasetName string, events []StreamVectorEvent) (int, error) {
	if !isValidResourceName(datasetName) {
		return 0, errors.New("invalid dataset name")
	}
	if len(events) == 0 {
		return 0, nil
	}
	for i := range events {
		if len(events[i].Vector) == 0 && len(events[i].Embedding) > 0 {
			events[i].Vector = events[i].Embedding
		}
	}
	rec, err := buildStreamArrowBatch(s.mem, events)
	if err != nil {
		return 0, err
	}
	defer rec.Release()

	if err := s.StoreRecordBatch(ctx, datasetName, rec); err != nil {
		return 0, err
	}
	s.workerMu.Lock()
	workersRunning := len(s.ingestionWorkerCancels) > 0
	s.workerMu.Unlock()
	if workersRunning {
		if ds, ok := s.getDataset(datasetName); ok {
			for ds.PendingIndexJobs.Load() > 0 || ds.PendingIngestion.Load() > 0 {
				select {
				case <-ctx.Done():
					return len(events), ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
	}
	return len(events), nil
}

// DeleteVectorByID marks a vector ID as deleted (tombstoned) in the target dataset.
func (s *VectorStore) DeleteVectorByID(_ context.Context, datasetName, rawID string) error {
	ds, ok := s.getDataset(datasetName)
	if !ok {
		return NewNotFoundError("dataset", datasetName)
	}
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	loc, found := resolveVectorLocationLocked(ds, rawID)
	if !found {
		return NewNotFoundError("vector", rawID)
	}
	if ds.Tombstones[loc.BatchIdx] == nil {
		ds.Tombstones[loc.BatchIdx] = types.NewBitset()
	}
	ds.Tombstones[loc.BatchIdx].Set(loc.RowIdx)
	metrics.TombstonesTotal.WithLabelValues(datasetName).Inc()
	return nil
}

// GetVectorByID fetches a stored vector and its text/metadata fields by ID.
func (s *VectorStore) GetVectorByID(datasetName, rawID string) (map[string]any, error) {
	ds, ok := s.getDataset(datasetName)
	if !ok {
		return nil, NewNotFoundError("dataset", datasetName)
	}
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	loc, found := resolveVectorLocationLocked(ds, rawID)
	if !found {
		return nil, NewNotFoundError("vector", rawID)
	}
	if ts := ds.Tombstones[loc.BatchIdx]; ts != nil && ts.Contains(loc.RowIdx) {
		return nil, NewNotFoundError("vector", rawID)
	}
	batches := ds.Records.Read()
	if loc.BatchIdx < 0 || loc.BatchIdx >= len(batches) {
		return nil, NewNotFoundError("vector", rawID)
	}
	rec := batches[loc.BatchIdx]
	if loc.RowIdx < 0 || loc.RowIdx >= int(rec.NumRows()) {
		return nil, NewNotFoundError("vector", rawID)
	}

	out := map[string]any{
		"id":      rawID,
		"dataset": datasetName,
	}
	if vec, err := extractVectorFromCol(rec, loc.RowIdx); err == nil {
		out["vector"] = vec
	}
	schema := rec.Schema()
	for i, f := range schema.Fields() {
		if f.Name == "vector" || f.Name == "embedding" {
			continue
		}
		if col, ok := rec.Column(i).(*array.String); ok && !col.IsNull(loc.RowIdx) {
			val := col.Value(loc.RowIdx)
			if f.Name == "metadata" {
				var meta map[string]any
				if err := json.Unmarshal([]byte(val), &meta); err == nil {
					out["metadata"] = meta
					continue
				}
			}
			out[f.Name] = val
		}
	}
	return out, nil
}

func resolveVectorLocationLocked(ds *Dataset, rawID string) (RowLocation, bool) {
	if loc, ok := ds.PrimaryIndex[rawID]; ok {
		return loc, true
	}
	if nid, err := strconv.ParseInt(rawID, 10, 64); err == nil {
		if loc, ok := ds.NumericPrimaryIndex[nid]; ok {
			return loc, true
		}
	}
	if uid, err := strconv.ParseUint(rawID, 10, 64); err == nil {
		if loc, ok := ds.Uint64PrimaryIndex[uid]; ok {
			return loc, true
		}
	}
	if idxID, err := strconv.ParseUint(rawID, 10, 32); err == nil && ds.Index != nil {
		if rawLoc, ok := ds.Index.GetLocation(uint32(idxID)); ok {
			if l, ok := rawLoc.(Location); ok {
				return RowLocation(l), true
			}
		}
	}
	return RowLocation{}, false
}

// DatasetInfo summarizes dataset dimensions, row counts, and fields for REST/GraphQL inspection.
type DatasetInfo struct {
	Name       string   `json:"name"`
	RowCount   int64    `json:"row_count"`
	IndexCount int      `json:"index_count"`
	VectorDim  int      `json:"vector_dim"`
	Tombstones int      `json:"tombstones"`
	Fields     []string `json:"fields,omitempty"`
}

func (s *VectorStore) inspectDataset(name string) (*DatasetInfo, error) {
	ds, ok := s.getDataset(name)
	if !ok {
		return nil, NewNotFoundError("dataset", name)
	}
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var rows int64
	var vecDim int
	var fields []string
	for _, b := range ds.Records.Read() {
		rows += b.NumRows()
		if len(fields) == 0 && b.Schema() != nil {
			for _, f := range b.Schema().Fields() {
				fields = append(fields, f.Name)
			}
		}
		if vecDim == 0 {
			if vec, err := extractVectorFromCol(b, 0); err == nil {
				vecDim = len(vec)
			}
		}
	}
	tombCount := 0
	for _, ts := range ds.Tombstones {
		if ts != nil {
			tombCount += int(ts.Count()) // #nosec G115
		}
	}
	idxCount := 0
	if ds.Index != nil {
		idxCount = ds.Index.Len()
	}
	return &DatasetInfo{
		Name:       name,
		RowCount:   rows,
		IndexCount: idxCount,
		VectorDim:  vecDim,
		Tombstones: tombCount,
		Fields:     fields,
	}, nil
}

func (s *VectorStore) listDatasetInfos(ctx context.Context) []DatasetInfo {
	names := s.GetActiveDatasets()
	sort.Strings(names)
	out := make([]DatasetInfo, 0, len(names))
	for _, n := range names {
		if !s.CanTenantListDataset(ctx, n) {
			continue
		}
		if info, err := s.inspectDataset(n); err == nil {
			out = append(out, *info)
		}
	}
	return out
}

// RESTHealthHTTPHandler returns Liveness and active runtime metadata for /api/v1/health.
func (s *VectorStore) RESTHealthHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "health", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeRESTJSON(w, "health", r.Method, http.StatusOK, map[string]any{
			"status":          "ok",
			"active_datasets": len(s.GetActiveDatasets()),
			"namespaces":      len(s.ListNamespaces()),
			"cloud_clusters":  len(s.GetCloudControlPlane().ListClusters("")),
			"timestamp":       time.Now().UTC().Format(time.RFC3339),
		})
	})
}

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

func (s *VectorStore) namespacesHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusForbidden, err.Error())
				return
			}
			names := s.ListNamespaces()
			sort.Strings(names)
			writeRESTJSON(w, "namespaces", r.Method, http.StatusOK, map[string]any{
				"namespaces": names,
				"count":      len(names),
			})
		case http.MethodPost:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermWrite, ""); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusForbidden, err.Error())
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			if err := s.CreateNamespace(req.Name); err != nil {
				writeRESTError(w, "namespaces", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "namespaces", r.Method, http.StatusCreated, map[string]any{"name": req.Name, "status": "created"})
		default:
			writeRESTError(w, "namespaces", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) namespaceItemHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/")
		name, _ = url.PathUnescape(strings.Trim(name, "/"))
		if !isValidResourceName(name) || strings.Contains(name, "/") {
			writeRESTError(w, "namespace_item", r.Method, http.StatusBadRequest, "invalid namespace name")
			return
		}
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "namespace_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			if !s.NamespaceExists(name) {
				writeRESTError(w, "namespace_item", r.Method, http.StatusNotFound, "namespace not found")
				return
			}
			writeRESTJSON(w, "namespace_item", r.Method, http.StatusOK, map[string]any{
				"name":     name,
				"datasets": s.ListDatasetsInNamespace(name),
			})
		case http.MethodDelete:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermWrite, ""); err != nil {
				writeRESTError(w, "namespace_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			if err := s.DeleteNamespace(name); err != nil {
				writeRESTError(w, "namespace_item", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "namespace_item", r.Method, http.StatusOK, map[string]any{"name": name, "status": "deleted"})
		default:
			writeRESTError(w, "namespace_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) datasetsCollectionHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "datasets", r.Method, http.StatusForbidden, err.Error())
				return
			}
			list := s.listDatasetInfos(r.Context())
			writeRESTJSON(w, "datasets", r.Method, http.StatusOK, map[string]any{"datasets": list, "count": len(list)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Name    string              `json:"name"`
				Vectors []StreamVectorEvent `json:"vectors,omitempty"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "datasets", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			if !isValidResourceName(req.Name) {
				writeRESTError(w, "datasets", r.Method, http.StatusBadRequest, "invalid dataset name")
				return
			}
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, req.Name); err != nil {
				writeRESTError(w, "datasets", r.Method, http.StatusForbidden, err.Error())
				return
			}
			inserted := 0
			if len(req.Vectors) > 0 {
				var err error
				inserted, err = s.UpsertStreamEvents(r.Context(), req.Name, req.Vectors)
				if err != nil {
					writeRESTError(w, "datasets", r.Method, http.StatusBadRequest, err.Error())
					return
				}
			} else {
				_, _ = s.getOrCreateDataset(req.Name, func() *Dataset {
					d := NewDataset(req.Name, nil)
					d.Logger = s.logger
					return d
				})
			}
			writeRESTJSON(w, "datasets", r.Method, http.StatusCreated, map[string]any{
				"name":     req.Name,
				"inserted": inserted,
				"status":   "created",
			})
		default:
			writeRESTError(w, "datasets", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) datasetItemHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawPath := strings.TrimPrefix(r.URL.Path, "/api/v1/datasets/")
		rawPath = strings.TrimSuffix(rawPath, "/")
		if rawPath == "" {
			writeRESTError(w, "dataset_item", r.Method, http.StatusBadRequest, "missing dataset path")
			return
		}

		// Handle /api/v1/datasets/{name}/vectors/{id}
		if idx := strings.Index(rawPath, "/vectors/"); idx != -1 {
			dsName, _ := url.PathUnescape(rawPath[:idx])
			vecID, _ := url.PathUnescape(rawPath[idx+len("/vectors/"):])
			s.handleDatasetVectorByID(w, r, dsName, vecID)
			return
		}

		for _, suffix := range []string{
			"/vectors",
			"/search/by-id",
			"/search",
			"/hybrid",
			"/text-search",
			"/geo-search",
			"/temporal-search",
			"/multimodal-search",
			"/multimodal-ingest",
			"/range-search",
			"/recommend",
			"/aggregations",
			"/bm25",
			"/facets",
			"/explain",
			"/provenance",
			"/schema/migrate",
			"/schema",
		} {
			if strings.HasSuffix(rawPath, suffix) {
				dsName, _ := url.PathUnescape(strings.TrimSuffix(rawPath, suffix))
				s.handleDatasetSubAction(w, r, dsName, suffix)
				return
			}
		}

		dsName, _ := url.PathUnescape(rawPath)
		if !isValidResourceName(dsName) {
			writeRESTError(w, "dataset_item", r.Method, http.StatusBadRequest, "invalid dataset name")
			return
		}
		switch r.Method {
		case http.MethodGet:
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			info, err := s.inspectDataset(dsName)
			if err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "dataset_item", r.Method, http.StatusOK, info)
		case http.MethodDelete:
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusForbidden, err.Error())
				return
			}
			if err := s.DropDataset(r.Context(), dsName); err != nil {
				writeRESTError(w, "dataset_item", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "dataset_item", r.Method, http.StatusOK, map[string]any{"name": dsName, "status": "deleted"})
		default:
			writeRESTError(w, "dataset_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) handleDatasetVectorByID(w http.ResponseWriter, r *http.Request, dsName, vecID string) {
	if !isValidResourceName(dsName) || vecID == "" {
		writeRESTError(w, "vector_item", r.Method, http.StatusBadRequest, "invalid dataset or vector id")
		return
	}
	switch r.Method {
	case http.MethodGet:
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusForbidden, err.Error())
			return
		}
		vec, err := s.GetVectorByID(dsName, vecID)
		if err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "vector_item", r.Method, http.StatusOK, vec)
	case http.MethodDelete:
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusForbidden, err.Error())
			return
		}
		if err := s.DeleteVectorByID(r.Context(), dsName, vecID); err != nil {
			writeRESTError(w, "vector_item", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "vector_item", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "id": vecID, "status": "deleted"})
	default:
		writeRESTError(w, "vector_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *VectorStore) handleDatasetSubAction(w http.ResponseWriter, r *http.Request, dsName, action string) {
	if !isValidResourceName(dsName) {
		writeRESTError(w, action[1:], r.Method, http.StatusBadRequest, "invalid dataset name")
		return
	}
	allowsGet := action == "/schema" || action == "/provenance"
	if r.Method != http.MethodPost && (!allowsGet || r.Method != http.MethodGet) {
		writeRESTError(w, action[1:], r.Method, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)

	switch action {
	case "/vectors":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "vectors_upsert", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			Vectors []StreamVectorEvent `json:"vectors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "vectors_upsert", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		n, err := s.UpsertStreamEvents(r.Context(), dsName, req.Vectors)
		if err != nil {
			writeRESTError(w, "vectors_upsert", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "vectors_upsert", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "upserted": n})

	case "/search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			Vector  []float32     `json:"vector"`
			K       int           `json:"k"`
			Filters []core.Filter `json:"filters,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		res, err := s.SearchDatasetVectors(r.Context(), dsName, req.Vector, req.K, req.Filters)
		if err != nil {
			writeRESTError(w, "search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/hybrid":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "hybrid", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			Vector     []float32     `json:"vector"`
			TextQuery  string        `json:"text_query"`
			K          int           `json:"k"`
			Alpha      float32       `json:"alpha"`
			RRFK       int           `json:"rrf_k"`
			GraphAlpha float32       `json:"graph_alpha"`
			GraphDepth int           `json:"graph_depth"`
			RawHybrid  bool          `json:"raw_hybrid,omitempty"`
			Filters    []core.Filter `json:"filters,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "hybrid", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		if req.RRFK <= 0 {
			req.RRFK = 60
		}
		res, err := s.SearchHybridWithFilters(r.Context(), dsName, req.Vector, req.TextQuery, req.K, req.Alpha, req.RRFK, req.GraphAlpha, req.GraphDepth, req.RawHybrid, req.Filters)
		if err != nil {
			writeRESTError(w, "hybrid", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res = s.FilterResultsByRLS(r.Context(), dsName, res)
		writeRESTJSON(w, "hybrid", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/search/by-id":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "search_by_id", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req core.VectorSearchByIDRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "search_by_id", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		res, err := s.SearchByID(r.Context(), &req)
		if err != nil {
			writeRESTError(w, "search_by_id", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "search_by_id", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/text-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "text_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req AdvancedTextQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "text_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		resp, err := s.GetTextSearchManager().Search(r.Context(), req)
		if err != nil {
			writeRESTError(w, "text_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "text_search", r.Method, http.StatusOK, resp)

	case "/geo-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "geo_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req types.GeoSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "geo_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		if req.SearchType == "" {
			req.SearchType = "radius"
		}
		if req.K <= 0 {
			req.K = 10
		}
		res, err := s.GeoSearch(r.Context(), &req)
		if err != nil {
			writeRESTError(w, "geo_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "geo_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/temporal-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "temporal_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			SearchType string          `json:"search_type"`
			K          int             `json:"k"`
			Timestamp  int64           `json:"timestamp,omitempty"`
			StartTime  int64           `json:"start_time,omitempty"`
			EndTime    int64           `json:"end_time,omitempty"`
			WindowSize int             `json:"window_size,omitempty"`
			Duration   json.RawMessage `json:"duration,omitempty"`
			DurationNs int64           `json:"duration_nanos,omitempty"`
			Filters    []core.Filter   `json:"filters,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.SearchType == "" {
			req.SearchType = "as_of"
		}
		if req.K <= 0 {
			req.K = 10
		}
		dur := time.Duration(req.DurationNs)
		if len(req.Duration) > 0 && string(req.Duration) != "null" {
			var durStr string
			var durInt int64
			if uerr := json.Unmarshal(req.Duration, &durStr); uerr == nil {
				parsed, perr := time.ParseDuration(durStr)
				if perr != nil {
					writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, perr.Error())
					return
				}
				dur = parsed
			} else if uerr := json.Unmarshal(req.Duration, &durInt); uerr == nil {
				dur = time.Duration(durInt)
			} else {
				writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, "invalid duration format")
				return
			}
		}
		res, err := s.TemporalSearch(r.Context(), &types.TemporalSearchRequest{
			Dataset:    dsName,
			SearchType: req.SearchType,
			K:          req.K,
			Timestamp:  req.Timestamp,
			StartTime:  req.StartTime,
			EndTime:    req.EndTime,
			WindowSize: req.WindowSize,
			Duration:   dur,
			Filters:    req.Filters,
		})
		if err != nil {
			writeRESTError(w, "temporal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "temporal_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/multimodal-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "multimodal_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req MultiModalSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "multimodal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		res, err := s.GetMultiModalPipeline().Search(r.Context(), req)
		if err != nil {
			writeRESTError(w, "multimodal_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res = s.FilterResultsByRLS(r.Context(), dsName, res)
		writeRESTJSON(w, "multimodal_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/range-search":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "range_search", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req core.RangeSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "range_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		res, err := s.RangeSearch(r.Context(), dsName, req.Vector, req.Threshold, req.Filters, req.MaxResults)
		if err != nil {
			writeRESTError(w, "range_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "range_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res), "threshold": req.Threshold})

	case "/recommend":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "recommend", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			SeedIDs []string `json:"seed_ids"`
			K       int      `json:"k"`
			Alpha   float32  `json:"alpha"`
			MaxHops int      `json:"max_hops"`
			Decay   float32  `json:"decay"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "recommend", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		if req.Alpha == 0 {
			req.Alpha = 0.7
		}
		res, err := s.Recommend(r.Context(), &qry.RecommendRequest{
			Dataset: dsName,
			SeedIDs: req.SeedIDs,
			K:       req.K,
			Alpha:   req.Alpha,
			MaxHops: req.MaxHops,
			Decay:   req.Decay,
		})
		if err != nil {
			writeRESTError(w, "recommend", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res = s.FilterResultsByRLS(r.Context(), dsName, res)
		writeRESTJSON(w, "recommend", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})

	case "/aggregations":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "aggregations", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			GroupBy      []string          `json:"group_by"`
			Aggregations []AggSpec         `json:"aggregations"`
			FilterEquals map[string]string `json:"filter_equals,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "aggregations", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		rows, sourceRows, err := s.runDatasetAggregation(dsName, req.GroupBy, req.Aggregations, req.FilterEquals)
		if err != nil {
			writeRESTError(w, "aggregations", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "aggregations", r.Method, http.StatusOK, map[string]any{
			"dataset":     dsName,
			"source_rows": sourceRows,
			"rows":        rows,
		})

	case "/multimodal-ingest":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "multimodal_ingest", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			Items       []MultiModalItem `json:"items,omitempty"`
			ParquetPath string           `json:"parquet_path,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "multimodal_ingest", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		var n int
		var err error
		if req.ParquetPath != "" {
			n, err = s.GetMultiModalPipeline().IngestParquetFile(r.Context(), dsName, req.ParquetPath)
		} else {
			n, err = s.GetMultiModalPipeline().IngestItems(r.Context(), dsName, req.Items)
		}
		if err != nil {
			writeRESTError(w, "multimodal_ingest", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "multimodal_ingest", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "ingested": n})

	case "/bm25":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermManageTenant, dsName); err != nil {
			writeRESTError(w, "bm25", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			K1 float64 `json:"k1"`
			B  float64 `json:"b"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "bm25", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.GetTextSearchManager().SetBM25Params(dsName, BM25TuningParams{K1: req.K1, B: req.B}); err != nil {
			writeRESTError(w, "bm25", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "bm25", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "status": "bm25_tuned", "k1": req.K1, "b": req.B})

	case "/facets":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "facets", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req AdvancedTextQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "facets", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		resp, err := s.GetTextSearchManager().Search(r.Context(), req)
		if err != nil {
			writeRESTError(w, "facets", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "facets", r.Method, http.StatusOK, map[string]any{"facets": resp.Facets, "total_hits": resp.TotalHits})

	case "/explain":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
			writeRESTError(w, "explain", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req ExplainSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "explain", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Dataset = dsName
		exp, err := s.GetProvenanceManager().ExplainSearch(r.Context(), req)
		if err != nil {
			writeRESTError(w, "explain", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "explain", r.Method, http.StatusOK, exp)

	case "/provenance":
		if r.Method == http.MethodGet {
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
				writeRESTError(w, "provenance", r.Method, http.StatusForbidden, err.Error())
				return
			}
			vecID := r.URL.Query().Get("vector_id")
			if vecID != "" {
				prov, ok := s.GetProvenanceManager().Get(dsName, vecID)
				if !ok {
					writeRESTError(w, "provenance", r.Method, http.StatusNotFound, "provenance not found")
					return
				}
				writeRESTJSON(w, "provenance", r.Method, http.StatusOK, prov)
				return
			}
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			list := s.GetProvenanceManager().ListByDataset(dsName, limit)
			writeRESTJSON(w, "provenance", r.Method, http.StatusOK, map[string]any{"provenance": list, "count": len(list)})
			return
		}
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, dsName); err != nil {
			writeRESTError(w, "provenance", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var prov VectorProvenance
		if err := json.NewDecoder(r.Body).Decode(&prov); err != nil {
			writeRESTError(w, "provenance", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		prov.Dataset = dsName
		if err := s.GetProvenanceManager().Record(prov); err != nil {
			writeRESTError(w, "provenance", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "provenance", r.Method, http.StatusOK, map[string]any{"status": "recorded", "dataset": dsName, "vector_id": prov.VectorID})

	case "/schema":
		if r.Method == http.MethodGet {
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, dsName); err != nil {
				writeRESTError(w, "schema", r.Method, http.StatusForbidden, err.Error())
				return
			}
			spec, ok := s.GetSchemaRegistry().LatestSchema(dsName)
			if !ok {
				writeRESTError(w, "schema", r.Method, http.StatusNotFound, "no registered schema")
				return
			}
			writeRESTJSON(w, "schema", r.Method, http.StatusOK, spec)
			return
		}
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermManageTenant, dsName); err != nil {
			writeRESTError(w, "schema", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var spec DatasetSchemaSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			writeRESTError(w, "schema", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		spec.Dataset = dsName
		ver, err := s.GetSchemaRegistry().RegisterSchema(spec)
		if err != nil {
			writeRESTError(w, "schema", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "schema", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "version": ver})

	case "/schema/migrate":
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermManageTenant, dsName); err != nil {
			writeRESTError(w, "schema_migrate", r.Method, http.StatusForbidden, err.Error())
			return
		}
		var req struct {
			Target        DatasetSchemaSpec `json:"target"`
			AddStringCols map[string]string `json:"add_string_cols,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "schema_migrate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		req.Target.Dataset = dsName
		ds, ok := s.getDataset(dsName)
		if !ok {
			ver, err := s.GetSchemaRegistry().RegisterSchema(req.Target)
			if err != nil {
				writeRESTError(w, "schema_migrate", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "schema_migrate", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "version": ver})
			return
		}
		ver, err := s.GetSchemaRegistry().MigrateDatasetSchema(ds, req.Target, req.AddStringCols)
		if err != nil {
			writeRESTError(w, "schema_migrate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "schema_migrate", r.Method, http.StatusOK, map[string]any{"dataset": dsName, "version": ver})
	}
}

func (s *VectorStore) federatedSearchHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "federated_search", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req core.FederatedSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "federated_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		res, err := s.FederatedSearch(r.Context(), req.Collections, req.Tags, req.Vector, req.K, req.RRFK)
		if err != nil {
			writeRESTError(w, "federated_search", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "federated_search", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res)})
	})
}

func (s *VectorStore) federatedCollectionsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		router := s.GetFederatedRouter()
		switch r.Method {
		case http.MethodGet:
			s.syncDatasetToFederatedRouter(router, s.GetActiveDatasets())
			cols := router.ListCollections()
			visible := make([]*CollectionInfo, 0, len(cols))
			for _, c := range cols {
				if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, c.Name); err == nil {
					visible = append(visible, c)
				}
			}
			writeRESTJSON(w, "federated_collections", r.Method, http.StatusOK, map[string]any{"collections": visible, "count": len(visible)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Dataset     string            `json:"dataset"`
				Name        string            `json:"name"`
				Description string            `json:"description,omitempty"`
				Tags        []string          `json:"tags,omitempty"`
				Metadata    map[string]string `json:"metadata,omitempty"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "federated_collections", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			name := req.Dataset
			if name == "" {
				name = req.Name
			}
			if !isValidResourceName(name) {
				writeRESTError(w, "federated_collections", r.Method, http.StatusBadRequest, "invalid collection name")
				return
			}
			if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, name); err != nil {
				writeRESTError(w, "federated_collections", r.Method, http.StatusForbidden, err.Error())
				return
			}
			info := &CollectionInfo{Name: name, Description: req.Description, Tags: req.Tags, Metadata: req.Metadata}
			router.UpsertCollection(name, &storeDatasetSearcher{store: s, dataset: name}, info)
			for _, tag := range req.Tags {
				_ = router.RegisterRoutingRule(&RoutingRule{Tag: tag, Collection: name, Priority: 1})
			}
			writeRESTJSON(w, "federated_collections", r.Method, http.StatusOK, map[string]any{"status": "registered", "collection": name, "tags": req.Tags})
		default:
			writeRESTError(w, "federated_collections", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) runDatasetAggregation(dsName string, groupBy []string, aggs []AggSpec, filters map[string]string) ([]MaterializedViewRow, int64, error) {
	if len(aggs) == 0 {
		aggs = []AggSpec{{Alias: "count", Op: AggCount}}
	}
	if len(filters) == 0 {
		if snap, ok := s.GetMaterializedViewManager().TryPlanQuery(dsName, groupBy, aggs); ok && snap != nil {
			return snap.Rows, snap.SourceRows, nil
		}
	}
	ds, ok := s.getDataset(dsName)
	if !ok {
		return nil, 0, NewNotFoundError("dataset", dsName)
	}
	ds.dataMu.RLock()
	batches := ds.Records.Read()
	tombstones := ds.Tombstones
	ds.dataMu.RUnlock()

	rows, sourceRows := AggregateBatches(batches, tombstones, MaterializedViewSpec{
		SourceDataset: dsName,
		GroupBy:       groupBy,
		Aggregations:  aggs,
		FilterEquals:  filters,
	})
	return rows, sourceRows, nil
}

func (s *VectorStore) adminUsageHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "admin_usage", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		reports := s.GetUsageMeter().GetAllUsage()
		var totalQueries, totalVectors, totalStorage, totalEgress, totalWarnings, totalRejections int64
		var totalCost float64
		for _, rep := range reports {
			totalQueries += rep.Queries
			totalVectors += rep.VectorsIngested
			totalStorage += rep.CurrentStorageBytes
			totalEgress += rep.EgressBytes
			totalWarnings += rep.SoftWarnings
			totalRejections += rep.HardRejections
			totalCost += rep.EstimatedCost
		}
		writeRESTJSON(w, "admin_usage", r.Method, http.StatusOK, map[string]any{
			"tenant_count": len(reports),
			"summary": map[string]any{
				"total_queries":         totalQueries,
				"total_vectors":         totalVectors,
				"total_storage_bytes":   totalStorage,
				"total_egress_bytes":    totalEgress,
				"soft_quota_warnings":   totalWarnings,
				"hard_quota_rejections": totalRejections,
				"estimated_cost_usd":    totalCost,
			},
			"tenants":      reports,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		})
	})
}

func (s *VectorStore) rbacKeysHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mgr := s.ensureRBACManager()
		switch r.Method {
		case http.MethodGet:
			keys := mgr.ListAPIKeys()
			writeRESTJSON(w, "rbac_keys", r.Method, http.StatusOK, map[string]any{"keys": keys, "count": len(keys)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Name       string            `json:"name"`
				Role       string            `json:"role"`
				TenantID   string            `json:"tenant_id"`
				Namespace  string            `json:"namespace"`
				Datasets   []string          `json:"datasets"`
				RowFilters map[string]string `json:"row_filters"`
				TTLSeconds int64             `json:"ttl_seconds"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "rbac_keys", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			ak, raw, err := mgr.CreateAPIKeyWithOptions(APIKeyOptions{
				Name:       req.Name,
				Role:       Role(req.Role),
				TenantID:   req.TenantID,
				Namespace:  req.Namespace,
				Datasets:   req.Datasets,
				RowFilters: req.RowFilters,
				TTL:        time.Duration(req.TTLSeconds) * time.Second,
			})
			if err != nil {
				writeRESTError(w, "rbac_keys", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "rbac_keys", r.Method, http.StatusCreated, map[string]any{
				"key_id":     ak.KeyID,
				"api_key":    raw,
				"role":       ak.Role,
				"tenant_id":  ak.TenantID,
				"namespace":  ak.Namespace,
				"expires_at": ak.ExpiresAt,
			})
		case http.MethodDelete:
			keyID := r.URL.Query().Get("key_id")
			if keyID == "" {
				keyID = r.URL.Query().Get("api_key")
			}
			if err := mgr.RevokeAPIKey(keyID); err != nil {
				writeRESTError(w, "rbac_keys", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "rbac_keys", r.Method, http.StatusOK, map[string]any{"status": "revoked"})
		default:
			writeRESTError(w, "rbac_keys", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) rbacRLSHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rbac_rls", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var pol security.RLSPolicy
		if err := json.NewDecoder(r.Body).Decode(&pol); err != nil {
			writeRESTError(w, "rbac_rls", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.ensureRBACManager().RLSEvaluator().SetPolicy(pol); err != nil {
			writeRESTError(w, "rbac_rls", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "rbac_rls", r.Method, http.StatusOK, map[string]any{"status": "rls_policy_set"})
	})
}

func (s *VectorStore) rbacAuditHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "rbac_audit", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 100
		}
		entries := s.AuditLogger().RecentEntries(limit)
		writeRESTJSON(w, "rbac_audit", r.Method, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
	})
}

func (s *VectorStore) backupsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			ds := r.URL.Query().Get("dataset")
			if s.backupManager == nil {
				writeRESTJSON(w, "backups", r.Method, http.StatusOK, map[string]any{"backups": []any{}, "count": 0})
				return
			}
			backups := s.backupManager.ListBackups(ds)
			writeRESTJSON(w, "backups", r.Method, http.StatusOK, map[string]any{"backups": backups, "count": len(backups)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				Dataset     string `json:"dataset"`
				Destination string `json:"destination"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "backups", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			meta, err := s.CreateOnlineBackup(r.Context(), req.Dataset, req.Destination)
			if err != nil {
				writeRESTError(w, "backups", r.Method, http.StatusInternalServerError, err.Error())
				return
			}
			writeRESTJSON(w, "backups", r.Method, http.StatusCreated, meta)
		default:
			writeRESTError(w, "backups", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) backupsRestoreHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "backups_restore", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			BackupID      string `json:"backup_id"`
			Dataset       string `json:"dataset"`
			WALDir        string `json:"wal_dir"`
			PITRTimestamp int64  `json:"pitr_timestamp"`
			MaxSeq        uint64 `json:"max_seq"`
			VerifyOnly    bool   `json:"verify_only"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "backups_restore", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		var targetTime time.Time
		if req.PITRTimestamp > 0 {
			targetTime = time.Unix(0, req.PITRTimestamp)
		}
		rep, err := s.RestoreWithPITR(r.Context(), PITRConfig{
			BackupID:     req.BackupID,
			DatasetName:  req.Dataset,
			WALDir:       req.WALDir,
			TargetTime:   targetTime,
			TargetMaxSeq: req.MaxSeq,
			VerifyOnly:   req.VerifyOnly,
		})
		if err != nil {
			writeRESTError(w, "backups_restore", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "backups_restore", r.Method, http.StatusOK, rep)
	})
}

func (s *VectorStore) backupsVerifyHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "backups_verify", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			BackupID string `json:"backup_id"`
			Dataset  string `json:"dataset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "backups_verify", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		rep, err := s.RestoreWithPITR(r.Context(), PITRConfig{
			BackupID:    req.BackupID,
			DatasetName: req.Dataset,
			VerifyOnly:  true,
		})
		if err != nil {
			writeRESTError(w, "backups_verify", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "backups_verify", r.Method, http.StatusOK, rep)
	})
}

func (s *VectorStore) streamsConnectorsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			stats := s.GetStreamManager().ListStats()
			writeRESTJSON(w, "stream_connectors", r.Method, http.StatusOK, map[string]any{"connectors": stats, "count": len(stats)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req struct {
				StreamConnectorConfig
				Start bool `json:"start"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "stream_connectors", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			conn, err := s.GetStreamManager().CreateConnector(req.StreamConnectorConfig, req.Start)
			if err != nil {
				writeRESTError(w, "stream_connectors", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "stream_connectors", r.Method, http.StatusCreated, conn.Stats())
		case http.MethodDelete:
			name := r.URL.Query().Get("name")
			if name == "" {
				var req struct {
					Name string `json:"name"`
				}
				_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req)
				name = req.Name
			}
			if err := s.GetStreamManager().StopConnector(name); err != nil {
				writeRESTError(w, "stream_connectors", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "stream_connectors", r.Method, http.StatusOK, map[string]any{"status": "stopped", "name": name})
		default:
			writeRESTError(w, "stream_connectors", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) streamsIngestHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Connector string           `json:"connector"`
			Envelopes []StreamEnvelope `json:"envelopes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		conn, ok := s.GetStreamManager().GetConnector(req.Connector)
		if !ok {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusNotFound, fmt.Sprintf("connector %q not found", req.Connector))
			return
		}
		committed, deduped, err := conn.ProcessEnvelopes(r.Context(), req.Envelopes)
		if err != nil {
			writeRESTError(w, "stream_ingest", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "stream_ingest", r.Method, http.StatusOK, map[string]any{
			"committed":    committed,
			"deduplicated": deduped,
			"stats":        conn.Stats(),
		})
	})
}

func (s *VectorStore) streamsSchemasHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "stream_schemas", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req RegisteredStreamSchema
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "stream_schemas", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		id := s.GetStreamManager().Registry().RegisterLocal(req.Subject, req)
		writeRESTJSON(w, "stream_schemas", r.Method, http.StatusCreated, map[string]any{"schema_id": id, "subject": req.Subject})
	})
}

func (s *VectorStore) rerankHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rerank", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Strategy string         `json:"strategy"`
			Query    string         `json:"query"`
			Results  []SearchResult `json:"results"`
			TopK     int            `json:"top_k"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "rerank", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		tenant := resolveTenantIDFromCtx(r.Context(), "")
		res, cacheHit, err := s.GetRerankManager().Rerank(r.Context(), req.Strategy, req.Query, req.Results, tenant)
		if err != nil {
			writeRESTError(w, "rerank", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if req.TopK > 0 && req.TopK < len(res) {
			res = res[:req.TopK]
		}
		writeRESTJSON(w, "rerank", r.Method, http.StatusOK, map[string]any{"results": res, "count": len(res), "cache_hit": cacheHit})
	})
}

func (s *VectorStore) rerankEvaluateHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rerank_evaluate", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Strategy string          `json:"strategy"`
			Cases    []EvalQueryCase `json:"cases"`
			K        int             `json:"k"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "rerank_evaluate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		var rr Reranker = s.GetRerankManager().hybrid
		if req.Strategy == "heuristic" {
			rr = &HeuristicReranker{ModelName: "default"}
		}
		rep, err := EvaluateReranker(r.Context(), rr, req.Cases, req.K)
		if err != nil {
			writeRESTError(w, "rerank_evaluate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "rerank_evaluate", r.Method, http.StatusOK, rep)
	})
}

func (s *VectorStore) rewriteQueryHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "rewrite_query", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Query string           `json:"query"`
			Mode  QueryRewriteMode `json:"mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "rewrite_query", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		variants := s.GetRerankManager().Rewrite(req.Query, req.Mode)
		writeRESTJSON(w, "rewrite_query", r.Method, http.StatusOK, map[string]any{"queries": variants})
	})
}

func (s *VectorStore) shardingTopologyHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "sharding_topology", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeRESTJSON(w, "sharding_topology", r.Method, http.StatusOK, s.GetElasticController().TopologySnapshot())
	})
}

func (s *VectorStore) shardingAutoScaleHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeRESTJSON(w, "sharding_autoscale", r.Method, http.StatusOK, s.GetElasticController().Policy())
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var pol ElasticScalingPolicy
			if err := json.NewDecoder(r.Body).Decode(&pol); err != nil {
				writeRESTError(w, "sharding_autoscale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			if err := s.GetElasticController().SetPolicy(pol); err != nil {
				writeRESTError(w, "sharding_autoscale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "sharding_autoscale", r.Method, http.StatusOK, map[string]any{"status": "policy_updated"})
		default:
			writeRESTError(w, "sharding_autoscale", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) shardingSplitHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "sharding_split", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Dataset string `json:"dataset"`
			ShardID string `json:"shard_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "sharding_split", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		left, right, migrated, err := s.GetElasticController().SplitShard(req.Dataset, req.ShardID)
		if err != nil {
			writeRESTError(w, "sharding_split", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "sharding_split", r.Method, http.StatusOK, map[string]any{"left": left, "right": right, "migrated": migrated})
	})
}

func (s *VectorStore) shardingMergeHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "sharding_merge", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Dataset      string `json:"dataset"`
			LeftShardID  string `json:"left_shard_id"`
			RightShardID string `json:"right_shard_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "sharding_merge", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		merged, moved, err := s.GetElasticController().MergeShards(req.Dataset, req.LeftShardID, req.RightShardID)
		if err != nil {
			writeRESTError(w, "sharding_merge", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "sharding_merge", r.Method, http.StatusOK, map[string]any{"merged": merged, "moved": moved})
	})
}

func (s *VectorStore) shardingRebalanceHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "sharding_rebalance", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Dataset string `json:"dataset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "sharding_rebalance", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		tasks, err := s.GetElasticController().RebalanceShards(req.Dataset)
		if err != nil {
			writeRESTError(w, "sharding_rebalance", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "sharding_rebalance", r.Method, http.StatusOK, map[string]any{"tasks": tasks, "count": len(tasks)})
	})
}

func (s *VectorStore) viewsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			views := s.GetMaterializedViewManager().ListViews()
			writeRESTJSON(w, "views", r.Method, http.StatusOK, map[string]any{"views": views, "count": len(views)})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var spec MaterializedViewSpec
			if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
				writeRESTError(w, "views", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			snap, err := s.GetMaterializedViewManager().CreateView(r.Context(), spec)
			if err != nil {
				writeRESTError(w, "views", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "views", r.Method, http.StatusCreated, snap)
		default:
			writeRESTError(w, "views", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) viewsQueryHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "views_query", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Name        string            `json:"name"`
			GroupFilter map[string]string `json:"group_filter,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "views_query", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		snap, err := s.GetMaterializedViewManager().QueryView(req.Name, req.GroupFilter)
		if err != nil {
			writeRESTError(w, "views_query", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "views_query", r.Method, http.StatusOK, snap)
	})
}

func (s *VectorStore) viewsRefreshHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "views_refresh", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRESTError(w, "views_refresh", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		snap, err := s.GetMaterializedViewManager().RefreshView(r.Context(), req.Name)
		if err != nil {
			writeRESTError(w, "views_refresh", r.Method, http.StatusNotFound, err.Error())
			return
		}
		writeRESTJSON(w, "views_refresh", r.Method, http.StatusOK, snap)
	})
}

func (s *VectorStore) searchAnalyticsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTError(w, "search_analytics", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		snap := s.GetTextSearchManager().Analytics(limit)
		writeRESTJSON(w, "search_analytics", r.Method, http.StatusOK, snap)
	})
}

func (s *VectorStore) cloudClustersCollectionHTTPHandler() http.Handler {
	ctrl := s.GetCloudControlPlane()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusForbidden, err.Error())
				return
			}
			tenantID := r.URL.Query().Get("tenant_id")
			clusters := ctrl.ListClusters(tenantID)
			writeRESTJSON(w, "cloud_clusters", r.Method, http.StatusOK, map[string]any{"clusters": clusters, "count": len(clusters)})
		case http.MethodPost:
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusForbidden, err.Error())
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			var req CreateCloudClusterRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.CreateCluster(req)
			if err != nil {
				writeRESTError(w, "cloud_clusters", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_clusters", r.Method, http.StatusCreated, cl)
		default:
			writeRESTError(w, "cloud_clusters", r.Method, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *VectorStore) cloudClusterItemHTTPHandler() http.Handler {
	ctrl := s.GetCloudControlPlane()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trimmed := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/cloud/clusters/"), "/")
		parts := strings.Split(trimmed, "/")
		if len(parts) == 0 || parts[0] == "" {
			writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusBadRequest, "missing cluster id")
			return
		}
		clusterID := parts[0]
		sub := ""
		if len(parts) > 1 {
			sub = parts[1]
		}

		switch sub {
		case "":
			switch r.Method {
			case http.MethodGet:
				if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusForbidden, err.Error())
					return
				}
				cl, ok := ctrl.GetCluster(clusterID)
				if !ok {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusNotFound, "cluster not found")
					return
				}
				writeRESTJSON(w, "cloud_cluster_item", r.Method, http.StatusOK, cl)
			case http.MethodDelete:
				if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusForbidden, err.Error())
					return
				}
				if err := ctrl.DeleteCluster(clusterID); err != nil {
					writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusNotFound, err.Error())
					return
				}
				writeRESTJSON(w, "cloud_cluster_item", r.Method, http.StatusOK, map[string]any{"deleted": clusterID})
			default:
				writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			}

		case "scale":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var req struct {
				Nodes int       `json:"nodes"`
				Tier  CloudTier `json:"tier"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req); err != nil {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.ScaleCluster(clusterID, req.Nodes, req.Tier)
			if err != nil {
				writeRESTError(w, "cloud_scale", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_scale", r.Method, http.StatusOK, cl)

		case "regions":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var req struct {
				Region   string `json:"region"`
				Endpoint string `json:"endpoint"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req); err != nil {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.AddReplicaRegion(clusterID, req.Region, req.Endpoint)
			if err != nil {
				writeRESTError(w, "cloud_regions", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_regions", r.Method, http.StatusOK, cl)

		case "replicate":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermWrite, ""); err != nil {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var ev ReplicationEvent
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&ev); err != nil {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			n, err := ctrl.ReplicateMutation(r.Context(), clusterID, ev)
			if err != nil {
				writeRESTError(w, "cloud_replicate", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_replicate", r.Method, http.StatusOK, map[string]any{"replicated_regions": n})

		case "promote":
			if r.Method != http.MethodPost {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermManageTenant, ""); err != nil {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var req struct {
				TargetRegion string `json:"target_region"`
				Reason       string `json:"reason"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&req); err != nil {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			cl, err := ctrl.PromoteRegion(r.Context(), clusterID, req.TargetRegion, req.Reason)
			if err != nil {
				writeRESTError(w, "cloud_promote", r.Method, http.StatusBadRequest, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_promote", r.Method, http.StatusOK, cl)

		case "sla":
			if r.Method != http.MethodGet {
				writeRESTError(w, "cloud_sla", r.Method, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.requireHTTPGlobalPermission(r.Context(), PermRead, ""); err != nil {
				writeRESTError(w, "cloud_sla", r.Method, http.StatusForbidden, err.Error())
				return
			}
			var p99 float64
			if v := r.URL.Query().Get("p99_ms"); v != "" {
				p99, _ = strconv.ParseFloat(v, 64)
			}
			rep, err := ctrl.EvaluateSLA(clusterID, p99)
			if err != nil {
				writeRESTError(w, "cloud_sla", r.Method, http.StatusNotFound, err.Error())
				return
			}
			writeRESTJSON(w, "cloud_sla", r.Method, http.StatusOK, rep)

		default:
			writeRESTError(w, "cloud_cluster_item", r.Method, http.StatusNotFound, "unknown cloud sub-resource")
		}
	})
}

func (s *VectorStore) cloudInternalReplicateHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTError(w, "cloud_internal_replicate", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var ev ReplicationEvent
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)).Decode(&ev); err != nil {
			writeRESTError(w, "cloud_internal_replicate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.GetCloudControlPlane().ApplyIncomingReplication(r.Context(), ev); err != nil {
			writeRESTError(w, "cloud_internal_replicate", r.Method, http.StatusBadRequest, err.Error())
			return
		}
		writeRESTJSON(w, "cloud_internal_replicate", r.Method, http.StatusOK, map[string]any{"status": "applied", "lsn": ev.LSN})
	})
}

// ---------- Pure-Go GraphQL Lexer, Recursive-Descent Parser & Executor ----------

type gqlTokKind int

const (
	tokEOF gqlTokKind = iota
	tokName
	tokInt
	tokFloat
	tokString
	tokLBrace
	tokRBrace
	tokLParen
	tokRParen
	tokLBrack
	tokRBrack
	tokColon
	tokEqual
	tokBang
	tokDollar
)

type gqlTok struct {
	kind gqlTokKind
	val  string
}

func lexGraphQL(src string) ([]gqlTok, error) {
	var toks []gqlTok
	i := 0
	for i < len(src) {
		ch := src[i]
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == ',' {
			i++
			continue
		}
		if ch == '#' {
			for i < len(src) && src[i] != '\n' {
				i++
			}
			continue
		}
		switch ch {
		case '{':
			toks = append(toks, gqlTok{tokLBrace, "{"})
			i++
		case '}':
			toks = append(toks, gqlTok{tokRBrace, "}"})
			i++
		case '(':
			toks = append(toks, gqlTok{tokLParen, "("})
			i++
		case ')':
			toks = append(toks, gqlTok{tokRParen, ")"})
			i++
		case '[':
			toks = append(toks, gqlTok{tokLBrack, "["})
			i++
		case ']':
			toks = append(toks, gqlTok{tokRBrack, "]"})
			i++
		case ':':
			toks = append(toks, gqlTok{tokColon, ":"})
			i++
		case '=':
			toks = append(toks, gqlTok{tokEqual, "="})
			i++
		case '!':
			toks = append(toks, gqlTok{tokBang, "!"})
			i++
		case '$':
			toks = append(toks, gqlTok{tokDollar, "$"})
			i++
		case '"':
			start := i + 1
			i++
			var sb strings.Builder
			closed := false
			for i < len(src) {
				if src[i] == '\\' && i+1 < len(src) {
					sb.WriteByte(src[i+1])
					i += 2
					continue
				}
				if src[i] == '"' {
					i++
					closed = true
					break
				}
				sb.WriteByte(src[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated string literal at %d", start)
			}
			toks = append(toks, gqlTok{tokString, sb.String()})
		default:
			if unicode.IsLetter(rune(ch)) || ch == '_' {
				start := i
				for i < len(src) && (unicode.IsLetter(rune(src[i])) || unicode.IsDigit(rune(src[i])) || src[i] == '_') {
					i++
				}
				toks = append(toks, gqlTok{tokName, src[start:i]})
			} else if ch == '-' || unicode.IsDigit(rune(ch)) {
				start := i
				if ch == '-' {
					i++
				}
				isFloat := false
				for i < len(src) && (unicode.IsDigit(rune(src[i])) || src[i] == '.' || src[i] == 'e' || src[i] == 'E' || src[i] == '+' || src[i] == '-') {
					if src[i] == '.' || src[i] == 'e' || src[i] == 'E' {
						isFloat = true
					}
					if (src[i] == '+' || src[i] == '-') && i > start && src[i-1] != 'e' && src[i-1] != 'E' {
						break
					}
					i++
				}
				if isFloat {
					toks = append(toks, gqlTok{tokFloat, src[start:i]})
				} else {
					toks = append(toks, gqlTok{tokInt, src[start:i]})
				}
			} else {
				return nil, fmt.Errorf("unexpected character %q at offset %d", ch, i)
			}
		}
	}
	toks = append(toks, gqlTok{kind: tokEOF})
	return toks, nil
}

type gqlOp struct {
	opType   string // "query" or "mutation"
	name     string
	defaults map[string]any
	sels     []gqlSelection
}

type gqlSelection struct {
	alias string
	name  string
	args  map[string]any
	sels  []gqlSelection
}

type gqlVarRef string

type gqlParser struct {
	toks []gqlTok
	pos  int
}

func (p *gqlParser) peek() gqlTok {
	if p.pos >= len(p.toks) {
		return gqlTok{kind: tokEOF}
	}
	return p.toks[p.pos]
}

func (p *gqlParser) next() gqlTok {
	t := p.peek()
	if p.pos < len(p.toks) {
		p.pos++
	}
	return t
}

func (p *gqlParser) expect(k gqlTokKind) (gqlTok, error) {
	t := p.next()
	if t.kind != k {
		return t, fmt.Errorf("unexpected token %q (kind %d), expected kind %d", t.val, t.kind, k)
	}
	return t, nil
}

func parseGraphQL(src string) ([]gqlOp, error) {
	toks, err := lexGraphQL(src)
	if err != nil {
		return nil, err
	}
	p := &gqlParser{toks: toks}
	var ops []gqlOp
	for p.peek().kind != tokEOF {
		op := gqlOp{opType: "query", defaults: make(map[string]any)}
		if p.peek().kind == tokLBrace {
			sels, err := p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
			op.sels = sels
			ops = append(ops, op)
			continue
		}
		if p.peek().kind != tokName {
			return nil, fmt.Errorf("expected query or mutation, got %q", p.peek().val)
		}
		kw := p.next().val
		if kw != "query" && kw != "mutation" && kw != "subscription" {
			return nil, fmt.Errorf("unsupported operation %q", kw)
		}
		op.opType = kw
		if p.peek().kind == tokName {
			op.name = p.next().val
		}
		if p.peek().kind == tokLParen {
			if err := p.parseVarDefs(op.defaults); err != nil {
				return nil, err
			}
		}
		sels, err := p.parseSelectionSet()
		if err != nil {
			return nil, err
		}
		op.sels = sels
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return nil, errors.New("empty GraphQL document")
	}
	return ops, nil
}

func (p *gqlParser) parseVarDefs(defaults map[string]any) error {
	p.next() // '('
	for p.peek().kind != tokRParen {
		if _, err := p.expect(tokDollar); err != nil {
			return err
		}
		nameTok, err := p.expect(tokName)
		if err != nil {
			return err
		}
		if _, err := p.expect(tokColon); err != nil {
			return err
		}
		if err := p.skipTypeRef(); err != nil {
			return err
		}
		if p.peek().kind == tokEqual {
			p.next()
			v, err := p.parseValue()
			if err != nil {
				return err
			}
			defaults[nameTok.val] = v
		}
	}
	p.next() // ')'
	return nil
}

func (p *gqlParser) skipTypeRef() error {
	if p.peek().kind == tokLBrack {
		p.next()
		if err := p.skipTypeRef(); err != nil {
			return err
		}
		if _, err := p.expect(tokRBrack); err != nil {
			return err
		}
	} else {
		if _, err := p.expect(tokName); err != nil {
			return err
		}
	}
	if p.peek().kind == tokBang {
		p.next()
	}
	return nil
}

func (p *gqlParser) parseSelectionSet() ([]gqlSelection, error) {
	if _, err := p.expect(tokLBrace); err != nil {
		return nil, err
	}
	var sels []gqlSelection
	for p.peek().kind != tokRBrace {
		if p.peek().kind == tokEOF {
			return nil, errors.New("unterminated selection set")
		}
		nTok, err := p.expect(tokName)
		if err != nil {
			return nil, err
		}
		sel := gqlSelection{name: nTok.val, args: make(map[string]any)}
		if p.peek().kind == tokColon {
			p.next()
			fTok, err := p.expect(tokName)
			if err != nil {
				return nil, err
			}
			sel.alias = sel.name
			sel.name = fTok.val
		}
		if p.peek().kind == tokLParen {
			args, err := p.parseArgs()
			if err != nil {
				return nil, err
			}
			sel.args = args
		}
		if p.peek().kind == tokLBrace {
			sub, err := p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
			sel.sels = sub
		}
		sels = append(sels, sel)
	}
	p.next() // '}'
	return sels, nil
}

func (p *gqlParser) parseArgs() (map[string]any, error) {
	p.next() // '('
	out := make(map[string]any)
	for p.peek().kind != tokRParen {
		if p.peek().kind == tokEOF {
			return nil, errors.New("unterminated argument list")
		}
		nTok, err := p.expect(tokName)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokColon); err != nil {
			return nil, err
		}
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out[nTok.val] = v
	}
	p.next() // ')'
	return out, nil
}

func (p *gqlParser) parseValue() (any, error) {
	t := p.peek()
	switch t.kind {
	case tokDollar:
		p.next()
		nt, err := p.expect(tokName)
		if err != nil {
			return nil, err
		}
		return gqlVarRef(nt.val), nil
	case tokString:
		p.next()
		return t.val, nil
	case tokInt:
		p.next()
		v, err := strconv.ParseInt(t.val, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case tokFloat:
		p.next()
		v, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case tokName:
		p.next()
		switch t.val {
		case "true":
			return true, nil
		case "false":
			return false, nil
		case "null":
			return nil, nil
		default:
			return t.val, nil
		}
	case tokLBrack:
		p.next()
		var list []any
		for p.peek().kind != tokRBrack {
			if p.peek().kind == tokEOF {
				return nil, errors.New("unterminated list literal")
			}
			item, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			list = append(list, item)
		}
		p.next()
		return list, nil
	case tokLBrace:
		p.next()
		obj := make(map[string]any)
		for p.peek().kind != tokRBrace {
			if p.peek().kind == tokEOF {
				return nil, errors.New("unterminated input object literal")
			}
			kt, err := p.expect(tokName)
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokColon); err != nil {
				return nil, err
			}
			val, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			obj[kt.val] = val
		}
		p.next()
		return obj, nil
	default:
		return nil, fmt.Errorf("unexpected value token %q", t.val)
	}
}

func resolveGQLValue(v any, vars map[string]any) any {
	switch x := v.(type) {
	case gqlVarRef:
		return vars[string(x)]
	case []any:
		out := make([]any, len(x))
		for i, it := range x {
			out[i] = resolveGQLValue(it, vars)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = resolveGQLValue(val, vars)
		}
		return out
	default:
		return v
	}
}

func resolveGQLArgs(args, vars map[string]any) map[string]any {
	resolved := make(map[string]any, len(args))
	for k, v := range args {
		resolved[k] = resolveGQLValue(v, vars)
	}
	return resolved
}

func argString(args map[string]any, key string) string {
	v, ok := args[key]
	if !ok || v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func argInt(args map[string]any, key string, def int) int {
	v, ok := args[key]
	if !ok || v == nil {
		return def
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
	}
	return def
}

func argFloat32(args map[string]any, key string, def float32) float32 {
	v, ok := args[key]
	if !ok || v == nil {
		return def
	}
	switch n := v.(type) {
	case int:
		return float32(n)
	case int64:
		return float32(n)
	case float64:
		return float32(n)
	}
	return def
}

func argFloat64Slice(v any) []float32 {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]float32, 0, len(items))
	for _, it := range items {
		switch n := it.(type) {
		case float64:
			out = append(out, float32(n))
		case int64:
			out = append(out, float32(n))
		case int:
			out = append(out, float32(n))
		}
	}
	return out
}

func argStringSlice(v any) []string {
	switch items := v.(type) {
	case []string:
		return items
	case []any:
		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, fmt.Sprint(it))
		}
		return out
	default:
		return nil
	}
}

func projectSelectionSet(val any, sels []gqlSelection) any {
	if len(sels) == 0 || val == nil {
		return val
	}
	raw, err := json.Marshal(val)
	if err != nil {
		return val
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return val
	}
	return projectJSONValue(generic, sels)
}

func projectJSONValue(v any, sels []gqlSelection) any {
	if len(sels) == 0 || v == nil {
		return v
	}
	switch node := v.(type) {
	case []any:
		out := make([]any, len(node))
		for i, item := range node {
			out[i] = projectJSONValue(item, sels)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(sels))
		for _, sel := range sels {
			key := sel.name
			if sel.alias != "" {
				key = sel.alias
			}
			if val, ok := findCamelOrSnakeField(node, sel.name); ok {
				out[key] = projectJSONValue(val, sel.sels)
			} else {
				out[key] = nil
			}
		}
		return out
	default:
		return v
	}
}

func findCamelOrSnakeField(m map[string]any, name string) (any, bool) {
	if v, ok := m[name]; ok {
		return v, true
	}
	snake := toSnakeCase(name)
	if v, ok := m[snake]; ok {
		return v, true
	}
	return nil, false
}

func toSnakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func graphqlSchemaIntrospection() map[string]any {
	types := []map[string]any{
		{
			"name": "Query",
			"kind": "OBJECT",
			"fields": []map[string]any{
				{"name": "health", "type": "HealthStatus"},
				{"name": "namespaces", "type": "[String!]!"},
				{"name": "datasets", "type": "[DatasetInfo!]!"},
				{"name": "dataset", "type": "DatasetInfo"},
				{"name": "search", "type": "[SearchResult!]!"},
				{"name": "searchById", "type": "[SearchResult!]!"},
				{"name": "hybridSearch", "type": "[SearchResult!]!"},
				{"name": "textSearch", "type": "AdvancedTextSearchResponse"},
				{"name": "geoSearch", "type": "[SearchResult!]!"},
				{"name": "temporalSearch", "type": "[SearchResult!]!"},
				{"name": "multimodalSearch", "type": "[SearchResult!]!"},
				{"name": "rangeSearch", "type": "[SearchResult!]!"},
				{"name": "federatedSearch", "type": "[SearchResult!]!"},
				{"name": "federatedCollections", "type": "[CollectionInfo!]!"},
				{"name": "recommend", "type": "[SearchResult!]!"},
				{"name": "aggregate", "type": "[AggregationRow!]!"},
				{"name": "usage", "type": "[TenantUsageReport!]!"},
				{"name": "cloudClusters", "type": "[CloudCluster!]!"},
				{"name": "cloudSla", "type": "SLAReport"},
				{"name": "explainSearch", "type": "SearchExplanationReport"},
				{"name": "provenance", "type": "[VectorProvenance!]!"},
				{"name": "searchAnalytics", "type": "SearchAnalyticsSnapshot"},
				{"name": "materializedViews", "type": "[MaterializedViewSnapshot!]!"},
				{"name": "schemaRegistry", "type": "DatasetSchemaSpec"},
				{"name": "clusterTopology", "type": "ElasticTopologySnapshot"},
				{"name": "streamConnectors", "type": "[StreamConnectorStats!]!"},
				{"name": "backups", "type": "[BackupMetadata!]!"},
				{"name": "rewriteQuery", "type": "QueryRewriteResult"},
			},
		},
		{
			"name": "Mutation",
			"kind": "OBJECT",
			"fields": []map[string]any{
				{"name": "createNamespace", "type": "StatusResult"},
				{"name": "deleteNamespace", "type": "StatusResult"},
				{"name": "upsertVectors", "type": "UpsertResult"},
				{"name": "deleteDataset", "type": "StatusResult"},
				{"name": "deleteVector", "type": "StatusResult"},
				{"name": "createCloudCluster", "type": "CloudCluster"},
				{"name": "promoteCloudRegion", "type": "CloudCluster"},
				{"name": "recordProvenance", "type": "StatusResult"},
				{"name": "createMaterializedView", "type": "MaterializedViewSnapshot"},
				{"name": "refreshMaterializedView", "type": "MaterializedViewSnapshot"},
				{"name": "registerSchema", "type": "SchemaVersionResult"},
				{"name": "triggerBackup", "type": "BackupMetadata"},
				{"name": "restoreBackup", "type": "PITRRestoreReport"},
				{"name": "createStreamConnector", "type": "StreamConnectorStats"},
				{"name": "stopStreamConnector", "type": "StatusResult"},
				{"name": "setTenantQuota", "type": "StatusResult"},
				{"name": "triggerShardSplit", "type": "ShardSplitResult"},
				{"name": "triggerShardMerge", "type": "ShardMergeResult"},
				{"name": "triggerRebalance", "type": "ShardRebalanceResult"},
				{"name": "ingestMedia", "type": "UpsertResult"},
				{"name": "rerank", "type": "[RerankCandidate!]!"},
			},
		},
		{"name": "HealthStatus", "kind": "OBJECT"},
		{"name": "DatasetInfo", "kind": "OBJECT"},
		{"name": "SearchResult", "kind": "OBJECT"},
		{"name": "CloudCluster", "kind": "OBJECT"},
		{"name": "SLAReport", "kind": "OBJECT"},
	}
	return map[string]any{
		"queryType":    map[string]any{"name": "Query"},
		"mutationType": map[string]any{"name": "Mutation"},
		"types":        types,
	}
}

// ExecuteGraphQL parses and runs a GraphQL query/mutation against VectorStore.
func (s *VectorStore) ExecuteGraphQL(ctx context.Context, query string, variables map[string]any, opName string) map[string]any {
	ops, err := parseGraphQL(query)
	if err != nil {
		metrics.GraphQLQueriesTotal.WithLabelValues("parse", "error").Inc()
		return map[string]any{"errors": []map[string]any{{"message": err.Error()}}}
	}

	selected := ops[0]
	if opName != "" {
		found := false
		for _, op := range ops {
			if op.name == opName {
				selected = op
				found = true
				break
			}
		}
		if !found {
			metrics.GraphQLQueriesTotal.WithLabelValues(selected.opType, "error").Inc()
			return map[string]any{"errors": []map[string]any{{"message": fmt.Sprintf("operation %q not found", opName)}}}
		}
	}

	mergedVars := make(map[string]any, len(selected.defaults)+len(variables))
	for k, v := range selected.defaults {
		mergedVars[k] = v
	}
	for k, v := range variables {
		mergedVars[k] = v
	}

	data := make(map[string]any, len(selected.sels))
	var gqlErrs []map[string]any

	for _, sel := range selected.sels {
		outKey := sel.name
		if sel.alias != "" {
			outKey = sel.alias
		}
		args := resolveGQLArgs(sel.args, mergedVars)
		var val any
		var execErr error
		if selected.opType == "mutation" {
			val, execErr = s.execGQLMutationField(ctx, sel.name, args)
		} else {
			val, execErr = s.execGQLQueryField(ctx, sel.name, args)
		}
		if execErr != nil {
			gqlErrs = append(gqlErrs, map[string]any{
				"message": execErr.Error(),
				"path":    []string{outKey},
			})
			data[outKey] = nil
			continue
		}
		data[outKey] = projectSelectionSet(val, sel.sels)
	}

	statusLabel := "success"
	if len(gqlErrs) > 0 {
		statusLabel = "error"
	}
	metrics.GraphQLQueriesTotal.WithLabelValues(selected.opType, statusLabel).Inc()
	resp := map[string]any{"data": data}
	if len(gqlErrs) > 0 {
		resp["errors"] = gqlErrs
	}
	return resp
}

func (s *VectorStore) execGQLQueryField(ctx context.Context, field string, args map[string]any) (any, error) {
	switch field {
	case "__schema":
		return graphqlSchemaIntrospection(), nil
	case "__type":
		typeName := argString(args, "name")
		schema := graphqlSchemaIntrospection()
		for _, t := range schema["types"].([]map[string]any) {
			if t["name"] == typeName {
				return t, nil
			}
		}
		return nil, nil
	case "health":
		return map[string]any{
			"status":         "ok",
			"activeDatasets": len(s.GetActiveDatasets()),
			"namespaces":     len(s.ListNamespaces()),
			"cloudClusters":  len(s.GetCloudControlPlane().ListClusters("")),
		}, nil
	case "namespaces":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		names := s.ListNamespaces()
		sort.Strings(names)
		return names, nil
	case "datasets":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.listDatasetInfos(ctx), nil
	case "dataset":
		name := argString(args, "name")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, name); err != nil {
			return nil, err
		}
		return s.inspectDataset(name)
	case "search":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		vec := argFloat64Slice(args["vector"])
		k := argInt(args, "k", 10)
		return s.SearchDatasetVectors(ctx, ds, vec, k, nil)
	case "searchById":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.SearchByID(ctx, &core.VectorSearchByIDRequest{
			Dataset: ds,
			ID:      argString(args, "id"),
			K:       argInt(args, "k", 10),
		})
	case "hybridSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		vec := argFloat64Slice(args["vector"])
		textQ := argString(args, "text")
		k := argInt(args, "k", 10)
		alpha := argFloat32(args, "alpha", 0.5)
		rrfK := argInt(args, "rrfK", 60)
		res, err := s.SearchHybridWithFilters(ctx, ds, vec, textQ, k, alpha, rrfK, argFloat32(args, "graphAlpha", 0), argInt(args, "graphDepth", 0), false, nil)
		if err != nil {
			return nil, err
		}
		return s.FilterResultsByRLS(ctx, ds, res), nil
	case "textSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.GetTextSearchManager().Search(ctx, AdvancedTextQueryRequest{
			Dataset:      ds,
			Query:        argString(args, "query"),
			K:            argInt(args, "k", 10),
			FuzzyMaxDist: argInt(args, "fuzzyMaxDist", 0),
			FacetFields:  argStringSlice(args["facetFields"]),
		})
	case "geoSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		searchType := argString(args, "searchType")
		if searchType == "" {
			searchType = "radius"
		}
		var box *core.GeoBoundingBox
		if argFloat32(args, "maxLat", 0) != 0 || argFloat32(args, "minLat", 0) != 0 {
			box = &core.GeoBoundingBox{
				MinLat: float64(argFloat32(args, "minLat", 0)),
				MaxLat: float64(argFloat32(args, "maxLat", 0)),
				MinLon: float64(argFloat32(args, "minLon", 0)),
				MaxLon: float64(argFloat32(args, "maxLon", 0)),
			}
		}
		return s.GeoSearch(ctx, &types.GeoSearchRequest{
			Dataset:     ds,
			Center:      core.GeoPoint{Lat: float64(argFloat32(args, "lat", 0)), Lon: float64(argFloat32(args, "lon", 0))},
			RadiusKm:    float64(argFloat32(args, "radiusKm", 10)),
			Box:         box,
			K:           argInt(args, "k", 10),
			SearchType:  searchType,
			QueryVector: argFloat64Slice(args["vector"]),
		})
	case "temporalSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		searchType := argString(args, "searchType")
		if searchType == "" {
			searchType = "as_of"
		}
		return s.TemporalSearch(ctx, &types.TemporalSearchRequest{
			Dataset:    ds,
			SearchType: searchType,
			K:          argInt(args, "k", 10),
			Timestamp:  int64(argInt(args, "timestamp", 0)),
			StartTime:  int64(argInt(args, "startTime", 0)),
			EndTime:    int64(argInt(args, "endTime", 0)),
			WindowSize: argInt(args, "windowSize", 0),
		})
	case "multimodalSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		res, err := s.GetMultiModalPipeline().Search(ctx, MultiModalSearchRequest{
			Dataset:   ds,
			Modality:  MediaModality(argString(args, "modality")),
			TextQuery: argString(args, "textQuery"),
			K:         argInt(args, "k", 10),
		})
		if err != nil {
			return nil, err
		}
		return s.FilterResultsByRLS(ctx, ds, res), nil
	case "rangeSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.RangeSearch(ctx, ds, argFloat64Slice(args["vector"]), argFloat32(args, "threshold", 0.5), nil, argInt(args, "maxResults", 0))
	case "federatedSearch":
		return s.FederatedSearch(ctx, argStringSlice(args["collections"]), argStringSlice(args["tags"]), argFloat64Slice(args["vector"]), argInt(args, "k", 10), argInt(args, "rrfK", 0))
	case "federatedCollections":
		router := s.GetFederatedRouter()
		s.syncDatasetToFederatedRouter(router, s.GetActiveDatasets())
		cols := router.ListCollections()
		visible := make([]*CollectionInfo, 0, len(cols))
		for _, c := range cols {
			if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, c.Name); err == nil {
				visible = append(visible, c)
			}
		}
		return visible, nil
	case "recommend":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		res, err := s.Recommend(ctx, &qry.RecommendRequest{
			Dataset: ds,
			SeedIDs: argStringSlice(args["seedIds"]),
			K:       argInt(args, "k", 10),
			Alpha:   argFloat32(args, "alpha", 0.5),
			MaxHops: argInt(args, "maxHops", 2),
			Decay:   argFloat32(args, "decay", 0.5),
		})
		if err != nil {
			return nil, err
		}
		return s.FilterResultsByRLS(ctx, ds, res), nil
	case "aggregate":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		var groupBy []string
		if rawGB, ok := args["groupBy"].([]any); ok {
			for _, it := range rawGB {
				groupBy = append(groupBy, fmt.Sprint(it))
			}
		}
		rows, _, err := s.runDatasetAggregation(ds, groupBy, nil, nil)
		return rows, err
	case "usage":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		if t := argString(args, "tenant"); t != "" {
			return []TenantUsageReport{s.GetUsageMeter().GetTenantUsage(t)}, nil
		}
		return s.GetUsageMeter().GetAllUsage(), nil
	case "cloudClusters":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().ListClusters(argString(args, "tenant")), nil
	case "cloudSla":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().EvaluateSLA(argString(args, "clusterId"), float64(argFloat32(args, "observedP99Ms", 0)))
	case "explainSearch":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		return s.GetProvenanceManager().ExplainSearch(ctx, ExplainSearchRequest{
			Dataset:   ds,
			Vector:    argFloat64Slice(args["vector"]),
			TextQuery: argString(args, "textQuery"),
			K:         argInt(args, "k", 10),
			Alpha:     float64(argFloat32(args, "alpha", 0.5)),
		})
	case "provenance":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		if vecID := argString(args, "vectorId"); vecID != "" {
			prov, ok := s.GetProvenanceManager().Get(ds, vecID)
			if !ok {
				return []VectorProvenance{}, nil
			}
			return []VectorProvenance{prov}, nil
		}
		return s.GetProvenanceManager().ListByDataset(ds, argInt(args, "limit", 50)), nil
	case "searchAnalytics":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetTextSearchManager().Analytics(argInt(args, "limit", 20)), nil
	case "materializedViews":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		if name := argString(args, "name"); name != "" {
			snap, err := s.GetMaterializedViewManager().QueryView(name, nil)
			if err != nil {
				return nil, err
			}
			return []*MaterializedViewSnapshot{snap}, nil
		}
		return s.GetMaterializedViewManager().ListViews(), nil
	case "schemaRegistry":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, ds); err != nil {
			return nil, err
		}
		spec, ok := s.GetSchemaRegistry().LatestSchema(ds)
		if !ok {
			return nil, NewNotFoundError("schema", ds)
		}
		return spec, nil
	case "clusterTopology":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetElasticController().TopologySnapshot(), nil
	case "streamConnectors":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		return s.GetStreamManager().ListStats(), nil
	case "backups":
		if err := s.requireHTTPGlobalPermission(ctx, PermBackup, ""); err != nil {
			return nil, err
		}
		if s.backupManager == nil {
			return []any{}, nil
		}
		return s.backupManager.ListBackups(argString(args, "dataset")), nil
	case "rewriteQuery":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		variants := s.GetRerankManager().Rewrite(argString(args, "query"), QueryRewriteMode(argString(args, "mode")))
		return map[string]any{"queries": variants}, nil
	default:
		return nil, fmt.Errorf("unknown GraphQL query field %q", field)
	}
}

func (s *VectorStore) execGQLMutationField(ctx context.Context, field string, args map[string]any) (any, error) {
	switch field {
	case "createNamespace":
		if err := s.requireHTTPGlobalPermission(ctx, PermWrite, ""); err != nil {
			return nil, err
		}
		name := argString(args, "name")
		if err := s.CreateNamespace(name); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "created"}, nil
	case "deleteNamespace":
		if err := s.requireHTTPGlobalPermission(ctx, PermWrite, ""); err != nil {
			return nil, err
		}
		name := argString(args, "name")
		if err := s.DeleteNamespace(name); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "deleted"}, nil
	case "upsertVectors":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		rawItems, ok := args["vectors"].([]any)
		if !ok {
			return nil, errors.New("vectors array is required")
		}
		events := make([]StreamVectorEvent, 0, len(rawItems))
		for _, raw := range rawItems {
			m, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			var meta map[string]any
			if rm, ok := m["metadata"].(map[string]any); ok {
				meta = rm
			}
			textVal, _ := m["text"].(string)
			events = append(events, StreamVectorEvent{
				Op:       "upsert",
				ID:       fmt.Sprint(m["id"]),
				Vector:   argFloat64Slice(m["vector"]),
				Text:     textVal,
				Metadata: meta,
			})
		}
		n, err := s.UpsertStreamEvents(ctx, ds, events)
		if err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "upserted": n}, nil
	case "deleteDataset":
		name := argString(args, "name")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, name); err != nil {
			return nil, err
		}
		if err := s.DropDataset(ctx, name); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "deleted"}, nil
	case "deleteVector":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		id := argString(args, "id")
		if err := s.DeleteVectorByID(ctx, ds, id); err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "id": id, "status": "deleted"}, nil
	case "createCloudCluster":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().CreateCluster(CreateCloudClusterRequest{
			ID:            argString(args, "id"),
			Name:          argString(args, "name"),
			TenantID:      argString(args, "tenantId"),
			Tier:          CloudTier(argString(args, "tier")),
			PrimaryRegion: argString(args, "primaryRegion"),
			Nodes:         argInt(args, "nodes", 1),
		})
	case "promoteCloudRegion":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetCloudControlPlane().PromoteRegion(ctx, argString(args, "clusterId"), argString(args, "targetRegion"), argString(args, "reason"))
	case "recordProvenance":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		prov := VectorProvenance{
			Dataset:      ds,
			VectorID:     argString(args, "vectorId"),
			SourceURI:    argString(args, "sourceUri"),
			ChunkIndex:   argInt(args, "chunkIndex", 0),
			Model:        argString(args, "model"),
			ModelVersion: argString(args, "modelVersion"),
		}
		if err := s.GetProvenanceManager().Record(prov); err != nil {
			return nil, err
		}
		return map[string]any{"status": "recorded", "dataset": ds, "vectorId": prov.VectorID}, nil
	case "createMaterializedView":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetMaterializedViewManager().CreateView(ctx, MaterializedViewSpec{
			Name:          argString(args, "name"),
			SourceDataset: argString(args, "sourceDataset"),
			GroupBy:       argStringSlice(args["groupBy"]),
			Aggregations:  []AggSpec{{Alias: "count", Op: AggCount}},
			RefreshPolicy: ViewRefreshPolicy(argString(args, "refreshMode")),
		})
	case "refreshMaterializedView":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		return s.GetMaterializedViewManager().RefreshView(ctx, argString(args, "name"))
	case "registerSchema":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermManageTenant, ds); err != nil {
			return nil, err
		}
		ver, err := s.GetSchemaRegistry().RegisterSchema(DatasetSchemaSpec{
			Dataset:       ds,
			Compatibility: SchemaCompatibilityMode(argString(args, "compatibility")),
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "version": ver}, nil
	case "triggerBackup":
		if err := s.requireHTTPGlobalPermission(ctx, PermBackup, ""); err != nil {
			return nil, err
		}
		return s.CreateOnlineBackup(ctx, argString(args, "dataset"), argString(args, "destination"))
	case "restoreBackup":
		if err := s.requireHTTPGlobalPermission(ctx, PermBackup, ""); err != nil {
			return nil, err
		}
		return s.RestoreWithPITR(ctx, PITRConfig{
			BackupID:    argString(args, "backupId"),
			DatasetName: argString(args, "dataset"),
			VerifyOnly:  argString(args, "verifyOnly") == "true",
		})
	case "createStreamConnector":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		conn, err := s.GetStreamManager().CreateConnector(StreamConnectorConfig{
			Name:       argString(args, "name"),
			Dataset:    argString(args, "dataset"),
			SourceType: StreamSourceType(argString(args, "sourceType")),
			Brokers:    argString(args, "brokers"),
			Topic:      argString(args, "topic"),
		}, argString(args, "start") == "true")
		if err != nil {
			return nil, err
		}
		return conn.Stats(), nil
	case "stopStreamConnector":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		name := argString(args, "name")
		if err := s.GetStreamManager().StopConnector(name); err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "status": "stopped"}, nil
	case "setTenantQuota":
		if err := s.requireHTTPGlobalPermission(ctx, PermManageTenant, ""); err != nil {
			return nil, err
		}
		tenant := argString(args, "tenantId")
		if err := s.GetUsageMeter().SetQuota(tenant, TenantQuota{
			MaxQueriesPerWindow: int64(argInt(args, "maxQueries", 0)),
			MaxVectors:          int64(argInt(args, "maxVectors", 0)),
			MaxStorageBytes:     int64(argInt(args, "maxStorageBytes", 0)),
		}); err != nil {
			return nil, err
		}
		return map[string]any{"tenantId": tenant, "status": "quota_set"}, nil
	case "triggerShardSplit":
		if err := s.requireHTTPGlobalPermission(ctx, PermAdmin, ""); err != nil {
			return nil, err
		}
		left, right, migrated, err := s.GetElasticController().SplitShard(argString(args, "dataset"), argString(args, "shardId"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"left": left, "right": right, "migrated": migrated}, nil
	case "triggerShardMerge":
		if err := s.requireHTTPGlobalPermission(ctx, PermAdmin, ""); err != nil {
			return nil, err
		}
		merged, moved, err := s.GetElasticController().MergeShards(argString(args, "dataset"), argString(args, "leftShardId"), argString(args, "rightShardId"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"merged": merged, "moved": moved}, nil
	case "triggerRebalance":
		if err := s.requireHTTPGlobalPermission(ctx, PermAdmin, ""); err != nil {
			return nil, err
		}
		tasks, err := s.GetElasticController().RebalanceShards(argString(args, "dataset"))
		if err != nil {
			return nil, err
		}
		return map[string]any{"tasks": tasks, "count": len(tasks)}, nil
	case "ingestMedia":
		ds := argString(args, "dataset")
		if _, err := s.AuthorizeDatasetAccess(ctx, PermWrite, ds); err != nil {
			return nil, err
		}
		n, err := s.GetMultiModalPipeline().IngestItems(ctx, ds, []MultiModalItem{{
			ID:        argString(args, "id"),
			Modality:  MediaModality(argString(args, "modality")),
			Caption:   argString(args, "caption"),
			SourceURI: argString(args, "sourceUri"),
		}})
		if err != nil {
			return nil, err
		}
		return map[string]any{"dataset": ds, "upserted": n}, nil
	case "rerank":
		if err := s.requireHTTPGlobalPermission(ctx, PermRead, ""); err != nil {
			return nil, err
		}
		res, _, err := s.GetRerankManager().Rerank(ctx, argString(args, "strategy"), argString(args, "query"), nil, resolveTenantIDFromCtx(ctx, ""))
		return res, err
	default:
		return nil, fmt.Errorf("unknown GraphQL mutation field %q", field)
	}
}

// GraphQLHTTPHandler exposes POST / GET /api/v1/graphql endpoint.
func (s *VectorStore) GraphQLHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query         string         `json:"query"`
			OperationName string         `json:"operationName"`
			Variables     map[string]any `json:"variables"`
		}
		switch r.Method {
		case http.MethodGet:
			req.Query = r.URL.Query().Get("query")
			req.OperationName = r.URL.Query().Get("operationName")
			if rawVars := r.URL.Query().Get("variables"); rawVars != "" {
				_ = json.Unmarshal([]byte(rawVars), &req.Variables)
			}
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxRESTBodyBytes)
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeRESTError(w, "graphql", r.Method, http.StatusBadRequest, err.Error())
				return
			}
		default:
			writeRESTError(w, "graphql", r.Method, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if strings.TrimSpace(req.Query) == "" {
			writeRESTError(w, "graphql", r.Method, http.StatusBadRequest, "missing GraphQL query")
			return
		}
		resp := s.ExecuteGraphQL(r.Context(), req.Query, req.Variables, req.OperationName)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
		metrics.RESTRequestsTotal.WithLabelValues("graphql", r.Method, "200").Inc()
	})
}

func (s *VectorStore) handleGraphQLAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	if action.Type != "GraphQLQuery" {
		return false, nil
	}
	var req struct {
		Query         string         `json:"query"`
		OperationName string         `json:"operationName"`
		Variables     map[string]any `json:"variables"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
	}
	res := s.ExecuteGraphQL(stream.Context(), req.Query, req.Variables, req.OperationName)
	return true, sendJSONActionResult(stream, res)
}
