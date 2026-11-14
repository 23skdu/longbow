package store

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func doJSONReq(t *testing.T, client *http.Client, method, url, apiKey string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(method, url, &buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestRESTAPI_EndToEndCRUDAndSearch(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := srv.Client()

	code, health := doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/health", "", nil)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "ok", health["status"])

	code, spec := doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/openapi.json", "", nil)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "3.0.3", spec["openapi"])
	paths := spec["paths"].(map[string]any)
	require.Contains(t, paths, "/api/v1/graphql")
	require.Contains(t, paths, "/api/v1/cloud/clusters")

	// Namespace CRUD
	code, _ = doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/namespaces", "", map[string]string{"name": "ns1"})
	require.Equal(t, http.StatusCreated, code)
	code, nsList := doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/namespaces", "", nil)
	require.Equal(t, http.StatusOK, code)
	require.GreaterOrEqual(t, int(nsList["count"].(float64)), 2)

	// Upsert vectors via REST
	code, up := doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/datasets/articles/vectors", "", map[string]any{
		"vectors": []map[string]any{
			{"id": "a1", "vector": []float32{1, 0, 0, 0}, "text": "vector database indexing", "metadata": map[string]any{"topic": "db"}},
			{"id": "a2", "vector": []float32{0.9, 0.1, 0, 0}, "text": "hnsw graph search", "metadata": map[string]any{"topic": "db"}},
			{"id": "a3", "vector": []float32{0, 0, 1, 0}, "text": "cloud multi region", "metadata": map[string]any{"topic": "cloud"}},
		},
	})
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, float64(3), up["upserted"])

	// Inspect vector & dataset
	code, vecObj := doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/datasets/articles/vectors/a1", "", nil)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "a1", vecObj["id"])

	code, dsInfo := doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/datasets/articles", "", nil)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, float64(3), dsInfo["row_count"])

	// Dense search, hybrid search, recommend, and aggregations
	code, searchRes := doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/datasets/articles/search", "", map[string]any{
		"vector": []float32{1, 0, 0, 0},
		"k":      2,
	})
	require.Equal(t, http.StatusOK, code)
	require.NotEmpty(t, searchRes["results"])

	code, hybridRes := doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/datasets/articles/hybrid", "", map[string]any{
		"vector":     []float32{0.9, 0.1, 0, 0},
		"text_query": "hnsw graph",
		"k":          2,
		"alpha":      0.6,
	})
	require.Equal(t, http.StatusOK, code)
	require.NotEmpty(t, hybridRes["results"])

	code, recRes := doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/datasets/articles/recommend", "", map[string]any{
		"seed_ids": []string{"a1"},
		"k":        2,
		"alpha":    0.8,
	})
	require.Equal(t, http.StatusOK, code)
	require.NotEmpty(t, recRes["results"])

	code, aggRes := doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/datasets/articles/aggregations", "", map[string]any{
		"group_by":     []string{"text"},
		"aggregations": []map[string]any{{"alias": "cnt", "op": "count"}},
	})
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, float64(3), aggRes["source_rows"])

	// Delete vector and dataset
	code, _ = doJSONReq(t, c, http.MethodDelete, srv.URL+"/api/v1/datasets/articles/vectors/a3", "", nil)
	require.Equal(t, http.StatusOK, code)
	code, _ = doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/datasets/articles/vectors/a3", "", nil)
	require.Equal(t, http.StatusNotFound, code)

	// Admin usage summary endpoint (Item 9.5)
	_ = vs.GetUsageMeter().CheckAndConsumeQuery(context.Background(), "acme", 512)
	code, adminUsage := doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/admin/usage", "", nil)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, adminUsage, "summary")

	code, _ = doJSONReq(t, c, http.MethodDelete, srv.URL+"/api/v1/datasets/articles", "", nil)
	require.Equal(t, http.StatusOK, code)
	code, _ = doJSONReq(t, c, http.MethodDelete, srv.URL+"/api/v1/namespaces/ns1", "", nil)
	require.Equal(t, http.StatusOK, code)
}

func TestRESTAPI_HTTPAuthMiddlewareEnforcement(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(1)
	vs.StartIndexingWorkers(1)
	defer func() { _ = vs.Close() }()

	mux := http.NewServeMux()
	// Register handlers BEFORE EnableRBAC to verify dynamic runtime enforcement as well
	vs.RegisterHTTPHandlers(mux)

	rbac := NewRBACManager()
	vs.EnableRBAC(rbac)
	_, viewerKey, err := rbac.CreateAPIKeyWithOptions(APIKeyOptions{Name: "viewer", Role: RoleReader})
	require.NoError(t, err)
	_, adminKey, err := rbac.CreateAPIKeyWithOptions(APIKeyOptions{Name: "admin", Role: RoleAdmin})
	require.NoError(t, err)

	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := srv.Client()

	// Unauthenticated health succeeds, protected endpoints return 401 Unauthorized
	code, _ := doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/health", "", nil)
	require.Equal(t, http.StatusOK, code)

	for _, protected := range []string{"/api/v1/datasets", "/api/v1/graphql", "/api/v1/admin/usage", "/api/v1/cloud/clusters"} {
		code, _ = doJSONReq(t, c, http.MethodGet, srv.URL+protected, "", nil)
		require.Equal(t, http.StatusUnauthorized, code, "expected 401 on %s", protected)
	}

	// Viewer is allowed to read datasets, denied write/admin actions (403)
	code, _ = doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/datasets", viewerKey, nil)
	require.Equal(t, http.StatusOK, code)

	code, _ = doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/admin/usage", viewerKey, nil)
	require.Equal(t, http.StatusForbidden, code)

	code, _ = doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/datasets/secured_ds/vectors", viewerKey, map[string]any{
		"vectors": []map[string]any{{"id": "v1", "vector": []float32{1, 0, 0, 0}}},
	})
	require.Equal(t, http.StatusForbidden, code)

	code, _ = doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/cloud/clusters", viewerKey, CreateCloudClusterRequest{
		ID:            "cls-denied",
		Name:          "denied",
		PrimaryRegion: "us-east-1",
	})
	require.Equal(t, http.StatusForbidden, code)

	// Admin succeeds on all writes, reads, admin usage, and cloud control plane routes
	code, _ = doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/datasets/secured_ds/vectors", adminKey, map[string]any{
		"vectors": []map[string]any{{"id": "v1", "vector": []float32{1, 0, 0, 0}}},
	})
	require.Equal(t, http.StatusOK, code)

	code, _ = doJSONReq(t, c, http.MethodGet, srv.URL+"/api/v1/admin/usage", adminKey, nil)
	require.Equal(t, http.StatusOK, code)

	code, _ = doJSONReq(t, c, http.MethodPost, srv.URL+"/api/v1/cloud/clusters", adminKey, CreateCloudClusterRequest{
		ID:            "cls-ok",
		Name:          "ok",
		PrimaryRegion: "us-east-1",
	})
	require.Equal(t, http.StatusCreated, code)
}

func TestGraphQL_QueriesMutationsIntrospection(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 1. Mutation: createNamespace + upsertVectors + createCloudCluster
	code, mutResp := doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/graphql", "", map[string]any{
		"query": `mutation Seed($ds: String!, $vecs: [VectorInput!]!) {
			createNamespace(name: "gqlns") { name status }
			upsertVectors(dataset: $ds, vectors: $vecs) { dataset upserted }
			createCloudCluster(name: "gql-cloud", primaryRegion: "us-west-2", tier: "enterprise", nodes: 4) { id name primaryRegion nodes }
		}`,
		"variables": map[string]any{
			"ds": "gql_items",
			"vecs": []map[string]any{
				{"id": "g1", "vector": []float32{1, 0, 0, 0}, "text": "alpha document"},
				{"id": "g2", "vector": []float32{0, 1, 0, 0}, "text": "beta document"},
			},
		},
	})
	require.Equal(t, http.StatusOK, code)
	require.Nil(t, mutResp["errors"])
	mutData := mutResp["data"].(map[string]any)
	require.Equal(t, float64(2), mutData["upsertVectors"].(map[string]any)["upserted"])
	require.Equal(t, float64(4), mutData["createCloudCluster"].(map[string]any)["nodes"])

	// 2. Query with aliases, variables, and introspection
	code, qResp := doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/graphql", "", map[string]any{
		"query": `query Inspect($vec: [Float!]!, $k: Int = 1) {
			sysHealth: health { status activeDatasets }
			dataset(name: "gql_items") { name rowCount vectorDim }
			hits: search(dataset: "gql_items", vector: $vec, k: $k) { id score }
			__schema { queryType { name } mutationType { name } }
			__type(name: "Query") { name kind }
		}`,
		"variables": map[string]any{
			"vec": []float32{1, 0, 0, 0},
		},
	})
	require.Equal(t, http.StatusOK, code)
	require.Nil(t, qResp["errors"])
	qData := qResp["data"].(map[string]any)
	require.Equal(t, "ok", qData["sysHealth"].(map[string]any)["status"])
	require.Equal(t, float64(2), qData["dataset"].(map[string]any)["rowCount"])
	require.Len(t, qData["hits"].([]any), 1)
	require.Equal(t, "Query", qData["__type"].(map[string]any)["name"])

	// 3. Flight DoAction("GraphQLQuery") dispatch works identically
	actionBody, _ := json.Marshal(map[string]any{"query": `{ health { status } namespaces }`})
	stream := &mockDoActionServer{}
	handled, err := vs.handleFeatureAction(&flight.Action{Type: "GraphQLQuery", Body: actionBody}, stream)
	require.True(t, handled)
	require.NoError(t, err)
	require.Contains(t, string(stream.results[0].Body), `"status":"ok"`)
}

func TestRESTAndGraphQLExtendedRoadmapEndpoints(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(1)
	vs.StartIndexingWorkers(1)
	defer func() { _ = vs.Close() }()

	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 1. Register schema via REST POST /api/v1/datasets/ext_ds/schema
	code, schemaResp := doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/datasets/ext_ds/schema", "", map[string]any{
		"compatibility": "BACKWARD",
		"constraints": []map[string]any{
			{"field": "vector", "vector_dim": 4},
		},
	})
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, float64(1), schemaResp["version"])

	// 2. Verify StoreRecordBatch enforces registered schema constraint on wrong dimension
	code, _ = doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/datasets/ext_ds/vectors", "", map[string]any{
		"vectors": []map[string]any{
			{"id": "bad_dim", "vector": []float32{1, 0}},
		},
	})
	require.Equal(t, http.StatusBadRequest, code)

	// 3. Create a streaming materialized view before ingesting valid vectors
	code, _ = doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/views", "", map[string]any{
		"name":           "ext_by_cat",
		"source_dataset": "ext_ds",
		"group_by":       []string{"category"},
		"aggregations":   []map[string]any{{"alias": "count", "op": "COUNT"}},
		"refresh_mode":   "streaming",
	})
	require.Equal(t, http.StatusCreated, code)

	// 4. Ingest valid 4D vectors and verify streaming view auto-refreshes & planner serves /aggregations
	code, _ = doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/datasets/ext_ds/vectors", "", map[string]any{
		"vectors": []map[string]any{
			{"id": "v1", "vector": []float32{1, 0, 0, 0}, "text": "gpu cluster", "metadata": map[string]any{"category": "infra"}},
			{"id": "v2", "vector": []float32{0, 1, 0, 0}, "text": "cpu node", "metadata": map[string]any{"category": "infra"}},
		},
	})
	require.Equal(t, http.StatusOK, code)

	code, aggResp := doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/datasets/ext_ds/aggregations", "", map[string]any{
		"group_by": []string{"category"},
	})
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, float64(2), aggResp["source_rows"])

	// 5. Provenance, Explain, Sharding Topology, Search Analytics, and Extended GraphQL fields
	code, _ = doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/datasets/ext_ds/provenance", "", map[string]any{
		"vector_id":          "v1",
		"source_document_id": "doc-99",
		"embedding_model":    "bge-small",
	})
	require.Equal(t, http.StatusOK, code)

	code, expResp := doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/datasets/ext_ds/explain", "", map[string]any{
		"vector": []float32{1, 0, 0, 0},
		"k":      1,
	})
	require.Equal(t, http.StatusOK, code)
	require.NotEmpty(t, expResp["candidates"])

	code, gqlExt := doJSONReq(t, srv.Client(), http.MethodPost, srv.URL+"/api/v1/graphql", "", map[string]any{
		"query": `{
			provenance(dataset: "ext_ds", vectorId: "v1") { vectorId sourceDocumentId }
			materializedViews(name: "ext_by_cat") { sourceRows }
			schemaRegistry(dataset: "ext_ds") { version compatibility }
			clusterTopology { ringSize }
			searchAnalytics { totalQueries }
		}`,
	})
	require.Equal(t, http.StatusOK, code)
	require.Nil(t, gqlExt["errors"])
}

func FuzzGraphQLParser(f *testing.F) {
	f.Add(`{ health { status } }`)
	f.Add(`query Q($k: Int = 5) { search(dataset: "ds", vector: [1.0, 0.5], k: $k) { id score } }`)
	f.Add(`mutation M { deleteVector(dataset: "ds", id: "1") { status } }`)
	f.Add(`{ __schema { types { name kind } } }`)
	f.Add(`{ broken(arg: "unterminated`)

	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 32*1024*1024, 32*1024*1024, 10*time.Millisecond)
	vs.dataPath = f.TempDir()
	vs.StartIngestionWorkers(1)
	vs.StartIndexingWorkers(1)
	defer func() { _ = vs.Close() }()

	f.Fuzz(func(t *testing.T, query string) {
		_, _ = parseGraphQL(query)
		if len(query) < 256 {
			_ = vs.ExecuteGraphQL(context.Background(), query, nil, "")
		}
	})
}

func FuzzRESTPayloads(f *testing.F) {
	f.Add("/api/v1/health", `GET`, `{}`)
	f.Add("/api/v1/datasets/fuzzds/search", `POST`, `{"vector":[1,0],"k":2}`)
	f.Add("/api/v1/datasets/fuzzds/vectors", `POST`, `{"vectors":[{"id":"1","vector":[1,0]}]}`)
	f.Add("/api/v1/cloud/clusters", `POST`, `{"name":"c1","primary_region":"us-east-1"}`)

	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 32*1024*1024, 32*1024*1024, 10*time.Millisecond)
	vs.dataPath = f.TempDir()
	vs.StartIngestionWorkers(1)
	vs.StartIndexingWorkers(1)
	defer func() { _ = vs.Close() }()
	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)

	f.Fuzz(func(t *testing.T, path, method, body string) {
		if path == "" || path[0] != '/' {
			return
		}
		if method != http.MethodGet && method != http.MethodPost && method != http.MethodDelete {
			method = http.MethodPost
		}
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
	})
}
