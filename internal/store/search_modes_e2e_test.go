package store

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturingDoGetStream struct {
	*MockDoGetStream
	frames []*flight.FlightData
}

func newCapturingDoGetStream(ctx context.Context) *capturingDoGetStream {
	return &capturingDoGetStream{MockDoGetStream: NewMockDoGetStream(ctx)}
}

func (c *capturingDoGetStream) Send(data *flight.FlightData) error {
	c.frames = append(c.frames, data)
	return c.MockDoGetStream.Send(data)
}

func newSearchModesTestStore(t *testing.T) (*VectorStore, memory.Allocator) {
	t.Helper()
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.SetHybridSearchConfig(HybridSearchConfig{
		Enabled:     true,
		Alpha:       0.5,
		TextColumns: []string{"text", "category"},
		BM25:        DefaultBM25Config(),
		RRFk:        60,
	})
	tCfg := DefaultTemporalConfig()
	tCfg.Enabled = true
	vs.SetTemporalIndex(tCfg)
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	t.Cleanup(func() { _ = vs.Close() })
	return vs, mem
}

func buildSearchModesBatch(t testing.TB, alloc memory.Allocator, baseTs int64) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
		{Name: "geo_point", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64), Nullable: false},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32), Nullable: false},
	}, nil)

	idB := array.NewStringBuilder(alloc)
	defer idB.Release()
	catB := array.NewStringBuilder(alloc)
	defer catB.Release()
	textB := array.NewStringBuilder(alloc)
	defer textB.Release()
	tsB := array.NewTimestampBuilder(alloc, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer tsB.Release()
	geoB := array.NewFixedSizeListBuilder(alloc, 2, arrow.PrimitiveTypes.Float64)
	defer geoB.Release()
	geoValB := geoB.ValueBuilder().(*array.Float64Builder)
	vecB := array.NewFixedSizeListBuilder(alloc, 4, arrow.PrimitiveTypes.Float32)
	defer vecB.Release()
	vecValB := vecB.ValueBuilder().(*array.Float32Builder)

	rows := []struct {
		id   string
		cat  string
		text string
		ts   int64
		lat  float64
		lon  float64
		vec  []float32
	}{
		{"doc-0", "security", "encryption quantum zero trust mesh", baseTs - 300, 37.7749, -122.4194, []float32{1, 0, 0, 0}},
		{"doc-1", "security", "password authentication tokens", baseTs - 200, 37.7849, -122.4094, []float32{0.9, 0.1, 0, 0}},
		{"doc-2", "storage", "columnar apache arrow snapshot storage", baseTs - 100, 40.7128, -74.0060, []float32{0, 1, 0, 0}},
		{"doc-3", "network", "high throughput rdma transport", baseTs - 50, 37.7755, -122.4180, []float32{0, 0, 1, 0}},
	}
	for _, r := range rows {
		idB.Append(r.id)
		catB.Append(r.cat)
		textB.Append(r.text)
		tsB.Append(arrow.Timestamp(r.ts))
		geoB.Append(true)
		geoValB.Append(r.lat)
		geoValB.Append(r.lon)
		vecB.Append(true)
		for _, v := range r.vec {
			vecValB.Append(v)
		}
	}

	cols := []arrow.Array{idB.NewArray(), catB.NewArray(), textB.NewArray(), tsB.NewArray(), geoB.NewArray(), vecB.NewArray()}
	for _, c := range cols {
		defer c.Release()
	}
	return array.NewRecordBatch(schema, cols, int64(len(rows)))
}

func waitDatasetIndexed(t *testing.T, vs *VectorStore, dataset string, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset(dataset)
		if !ok || ds.IndexLen() < want {
			return false
		}
		if ds.TemporalIndex != nil && ds.TemporalIndex.Size() < want {
			return false
		}
		if ds.BM25Index != nil || ds.BM25ArenaIndex != nil {
			results, err := vs.SearchHybrid(context.Background(), dataset, nil, "encryption", 5, 0.0, 60, 0, 0, false)
			return err == nil && len(results) > 0
		}
		return true
	}, 5*time.Second, 25*time.Millisecond)
}

func TestSearchModes_PureSparseAndHybridFilters(t *testing.T) {
	vs, mem := newSearchModesTestStore(t)
	baseTs := time.Now().UnixNano()
	rec := buildSearchModesBatch(t, mem, baseTs)
	defer rec.Release()

	require.NoError(t, vs.StoreRecordBatch(context.Background(), "kb", rec))
	waitDatasetIndexed(t, vs, "kb", 4)

	// 1. Pure Sparse via HandleVectorSearchAction (Vector empty, TextQuery non-empty, Alpha=0.0)
	reqBody, _ := json.Marshal(query.VectorSearchRequest{
		Dataset:   "kb",
		TextQuery: "encryption",
		K:         3,
		Alpha:     0.0,
	})
	vsaStream := &mockVectorSearchDoActionServer{}
	err := vs.HandleVectorSearchAction(&flight.Action{Type: "VectorSearch", Body: reqBody}, vsaStream)
	require.NoError(t, err)
	require.NotEmpty(t, vsaStream.results, "pure sparse VectorSearch DoAction must return results")

	// 2. HybridSearch DoAction with filters (storage only)
	hybBody, _ := json.Marshal(map[string]any{
		"dataset":    "kb",
		"vector":     []float32{1, 0, 0, 0},
		"text_query": "encryption storage",
		"k":          5,
		"alpha":      0.5,
		"filters":    []query.Filter{{Field: "category", Operator: "=", Value: "storage"}},
	})
	hybStream := &mockVectorSearchDoActionServer{}
	err = vs.DoAction(&flight.Action{Type: "HybridSearch", Body: hybBody}, hybStream)
	require.NoError(t, err)
	require.Len(t, hybStream.results, 1)
	var hybRes []SearchResult
	require.NoError(t, json.Unmarshal(hybStream.results[0].Body, &hybRes))
	require.Len(t, hybRes, 1)
	assert.Equal(t, VectorID(2), hybRes[0].ID)
}

func TestSearchModes_RangeAndFederatedAndHTTP(t *testing.T) {
	vs, mem := newSearchModesTestStore(t)
	baseTs := time.Now().UnixNano()
	recA := buildSearchModesBatch(t, mem, baseTs)
	defer recA.Release()
	recB := buildSearchModesBatch(t, mem, baseTs)
	defer recB.Release()

	ctx := context.Background()
	require.NoError(t, vs.StoreRecordBatch(ctx, "col_a", recA))
	require.NoError(t, vs.StoreRecordBatch(ctx, "col_b", recB))
	waitDatasetIndexed(t, vs, "col_a", 4)
	waitDatasetIndexed(t, vs, "col_b", 4)

	// RangeSearch: tight threshold returns only close vector, loose threshold returns all capped by max_results
	tight, err := vs.RangeSearch(ctx, "col_a", []float32{1, 0, 0, 0}, 0.05, nil, 10)
	require.NoError(t, err)
	require.NotEmpty(t, tight)
	assert.LessOrEqual(t, len(tight), 2)

	loose, err := vs.RangeSearch(ctx, "col_a", []float32{1, 0, 0, 0}, 10.0, nil, 2)
	require.NoError(t, err)
	assert.Len(t, loose, 2)

	// RangeSearch DoGet
	ticketBytes, _ := json.Marshal(query.TicketQuery{
		RangeSearch: &query.RangeSearchRequest{
			Dataset:   "col_a",
			Vector:    []float32{1, 0, 0, 0},
			Threshold: 10.0,
		},
	})
	dgStream := newCapturingDoGetStream(ctx)
	require.NoError(t, vs.DoGet(&flight.Ticket{Ticket: ticketBytes}, dgStream))
	assert.NotEmpty(t, dgStream.frames)

	// FederatedSearch DoAction & tag routing
	regBody, _ := json.Marshal(map[string]any{"dataset": "col_a", "tags": []string{"sec"}})
	regStream := &mockVectorSearchDoActionServer{}
	require.NoError(t, vs.DoAction(&flight.Action{Type: "RegisterFederatedCollection", Body: regBody}, regStream))

	fedBody, _ := json.Marshal(query.FederatedSearchRequest{
		Tags:   []string{"sec"},
		Vector: []float32{1, 0, 0, 0},
		K:      3,
	})
	fedStream := &mockVectorSearchDoActionServer{}
	require.NoError(t, vs.DoAction(&flight.Action{Type: "FederatedSearch", Body: fedBody}, fedStream))
	require.NotEmpty(t, fedStream.results)

	// REST handlers + GraphQL + OpenAPI
	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)

	postJSON := func(path string, payload any) map[string]any {
		raw, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, "path=%s body=%s", path, w.Body.String())
		var out map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
		return out
	}

	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/range-search", map[string]any{
		"vector": []float32{1, 0, 0, 0}, "threshold": 5.0,
	})["count"].(float64)), 1)

	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/search/by-id", map[string]any{
		"id": "doc-0", "k": 2,
	})["count"].(float64)), 1)

	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/text-search", map[string]any{
		"query": "encryption AND mesh", "k": 3, "facet_fields": []string{"category"},
	})["total_hits"].(float64)), 1)

	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/geo-search", map[string]any{
		"search_type": "radius", "center": map[string]float64{"lat": 37.7749, "lon": -122.4194}, "radius_km": 5.0, "k": 5,
	})["count"].(float64)), 1)
	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/geo-search", map[string]any{
		"search_type": "box", "box": map[string]float64{"min_lat": 37.7, "max_lat": 37.8, "min_lon": -122.5, "max_lon": -122.4}, "k": 5,
	})["count"].(float64)), 1)
	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/geo-search", map[string]any{
		"search_type": "hybrid", "center": map[string]float64{"lat": 37.7749, "lon": -122.4194}, "radius_km": 5.0, "query_vector": []float32{1, 0, 0, 0}, "k": 5,
	})["count"].(float64)), 1)

	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/temporal-search", map[string]any{
		"search_type": "as_of", "timestamp": baseTs, "k": 4,
	})["count"].(float64)), 1)
	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/temporal-search", map[string]any{
		"search_type": "range", "start_time": baseTs - 400, "end_time": baseTs, "k": 4,
	})["count"].(float64)), 1)
	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/temporal-search", map[string]any{
		"search_type": "sliding_window", "window_size": 2, "k": 4,
	})["count"].(float64)), 1)
	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/col_a/temporal-search", map[string]any{
		"search_type": "sliding_window_time", "duration": int64(10 * time.Second), "k": 4,
	})["count"].(float64)), 1)

	_, err = vs.GetMultiModalPipeline().IngestItems(ctx, "media_ds", []MultiModalItem{
		{ID: "m-0", Modality: ModalityText, Caption: "quantum zero trust mesh encryption"},
		{ID: "m-1", Modality: ModalityText, Caption: "columnar apache arrow storage"},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("media_ds")
		return ok && ds.IndexLen() >= 2
	}, 5*time.Second, 25*time.Millisecond)
	assert.GreaterOrEqual(t, int(postJSON("/api/v1/datasets/media_ds/multimodal-search", map[string]any{
		"modality": "text", "text_query": "quantum", "k": 2,
	})["count"].(float64)), 1)

	assert.GreaterOrEqual(t, int(postJSON("/api/v1/search/federated", map[string]any{
		"collections": []string{"col_a", "col_b"}, "vector": []float32{1, 0, 0, 0}, "k": 3, "rrf_k": 30,
	})["count"].(float64)), 1)

	// OpenAPI lists new endpoints
	oaReq := httptest.NewRequest(http.MethodGet, "/api/v1/openapi.json", nil)
	oaW := httptest.NewRecorder()
	mux.ServeHTTP(oaW, oaReq)
	require.Equal(t, http.StatusOK, oaW.Code)
	assert.Contains(t, oaW.Body.String(), "/api/v1/datasets/{name}/range-search")
	assert.Contains(t, oaW.Body.String(), "/api/v1/search/federated")

	// GraphQL rangeSearch + federatedSearch
	gqlOut := vs.ExecuteGraphQL(ctx, `query {
		r: rangeSearch(dataset: "col_a", vector: [1, 0, 0, 0], threshold: 5.0) { id distance }
		f: federatedSearch(collections: ["col_a", "col_b"], vector: [1, 0, 0, 0], k: 2) { id score }
	}`, nil, "")
	assert.Nil(t, gqlOut["errors"])
	gqlData := gqlOut["data"].(map[string]any)
	assert.NotEmpty(t, gqlData["r"])
	assert.NotEmpty(t, gqlData["f"])
}

func TestSearchModes_All21Modalities(t *testing.T) {
	vs, mem := newSearchModesTestStore(t)
	baseTs := time.Now().UnixNano()
	ctx := context.Background()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "rank", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
		{Name: "geo_point", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64), Nullable: false},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32), Nullable: false},
	}, nil)

	idB := array.NewStringBuilder(mem)
	defer idB.Release()
	rankB := array.NewInt64Builder(mem)
	defer rankB.Release()
	activeB := array.NewBooleanBuilder(mem)
	defer activeB.Release()
	catB := array.NewStringBuilder(mem)
	defer catB.Release()
	textB := array.NewStringBuilder(mem)
	defer textB.Release()
	tsB := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer tsB.Release()
	geoB := array.NewFixedSizeListBuilder(mem, 2, arrow.PrimitiveTypes.Float64)
	defer geoB.Release()
	geoValB := geoB.ValueBuilder().(*array.Float64Builder)
	vecB := array.NewFixedSizeListBuilder(mem, 4, arrow.PrimitiveTypes.Float32)
	defer vecB.Release()
	vecValB := vecB.ValueBuilder().(*array.Float32Builder)

	rows := []struct {
		id     string
		rank   int64
		active bool
		cat    string
		text   string
		ts     int64
		lat    float64
		lon    float64
		vec    []float32
	}{
		{"0", 5, true, "security", "encryption quantum zero trust mesh", baseTs - 300, 37.7749, -122.4194, []float32{1, 0, 0, 0}},
		{"1", 15, true, "electronics", "solid state semiconductor storage", baseTs - 200, 37.7849, -122.4094, []float32{0.9, 0.1, 0, 0}},
		{"2", 25, false, "storage", "columnar apache arrow snapshot storage", baseTs - 100, 40.7128, -74.0060, []float32{0, 1, 0, 0}},
		{"3", 35, true, "network", "high throughput rdma transport", baseTs - 50, 37.7755, -122.4180, []float32{0, 0, 1, 0}},
	}
	for _, r := range rows {
		idB.Append(r.id)
		rankB.Append(r.rank)
		activeB.Append(r.active)
		catB.Append(r.cat)
		textB.Append(r.text)
		tsB.Append(arrow.Timestamp(r.ts))
		geoB.Append(true)
		geoValB.Append(r.lat)
		geoValB.Append(r.lon)
		vecB.Append(true)
		for _, v := range r.vec {
			vecValB.Append(v)
		}
	}
	cols := []arrow.Array{idB.NewArray(), rankB.NewArray(), activeB.NewArray(), catB.NewArray(), textB.NewArray(), tsB.NewArray(), geoB.NewArray(), vecB.NewArray()}
	for _, c := range cols {
		defer c.Release()
	}
	rec := array.NewRecordBatch(schema, cols, int64(len(rows)))
	defer rec.Release()

	require.NoError(t, vs.StoreRecordBatch(ctx, "all_modes", rec))
	waitDatasetIndexed(t, vs, "all_modes", 4)

	ds, ok := vs.getDataset("all_modes")
	require.True(t, ok)
	require.NotNil(t, ds.Graph)
	require.NoError(t, ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0}))
	require.NoError(t, ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "related", Object: 2, Weight: 0.9}))

	mux := http.NewServeMux()
	vs.RegisterHTTPHandlers(mux)

	doGetCount := func(name string, tq query.TicketQuery) int {
		t.Helper()
		raw, err := json.Marshal(tq)
		require.NoError(t, err, name)
		dg := newCapturingDoGetStream(ctx)
		require.NoError(t, vs.DoGet(&flight.Ticket{Ticket: raw}, dg), name)
		total := 0
		for _, f := range dg.frames {
			if len(f.DataBody) > 0 {
				total++
			}
		}
		require.GreaterOrEqual(t, total, 1, "mode %s returned no frames", name)
		return total
	}

	postJSON := func(name, path string, payload any) map[string]any {
		t.Helper()
		raw, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, "mode=%s path=%s body=%s", name, path, w.Body.String())
		var out map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
		return out
	}

	// 1. dense
	doGetCount("dense", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, K: 3,
	}})

	// 2. hybrid
	doGetCount("hybrid", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, TextQuery: "encryption", K: 3, Alpha: 0.5,
	}})

	// 3. sparse
	doGetCount("sparse", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", TextQuery: "encryption", K: 3, Alpha: 0.0,
	}})

	// 4. filtered (numeric predicate)
	doGetCount("filtered", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, K: 3,
		Filters: []query.Filter{{Field: "rank", Operator: ">", Value: "10"}},
	}})

	// 5. filtered_bool
	doGetCount("filtered_bool", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, K: 3,
		Filters: []query.Filter{{Field: "active", Operator: "==", Value: "true"}},
	}})

	// 6. filtered_string
	doGetCount("filtered_string", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, K: 3,
		Filters: []query.Filter{{Field: "category", Operator: "==", Value: "electronics"}},
	}})

	// 7. by_id
	assert.GreaterOrEqual(t, int(postJSON("by_id", "/api/v1/datasets/all_modes/search/by-id", map[string]any{
		"id": "0", "k": 2,
	})["count"].(float64)), 1)

	// 8. range
	doGetCount("range", query.TicketQuery{RangeSearch: &query.RangeSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, Threshold: 5.0,
	}})

	// 9. text (AdvancedTextSearch)
	assert.GreaterOrEqual(t, int(postJSON("text", "/api/v1/datasets/all_modes/text-search", map[string]any{
		"query": "encryption AND mesh", "k": 3, "facet_fields": []string{"category"},
	})["total_hits"].(float64)), 1)

	// 10. graphrag
	doGetCount("graphrag", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, K: 3, GraphAlpha: 0.5, GraphDepth: 2,
	}})

	// 11. global_graphrag (GlobalSearchCoordinator scatter-gather + GraphAlpha)
	coord := NewGlobalSearchCoordinator(zerolog.Nop(), nil)
	vs.SetCoordinator(coord)
	doGetCount("global_graphrag", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, K: 3, GraphAlpha: 0.5, LocalOnly: false,
	}})

	// 12. recommend
	assert.GreaterOrEqual(t, int(postJSON("recommend", "/api/v1/datasets/all_modes/recommend", map[string]any{
		"seed_ids": []string{"0", "1"}, "k": 2, "alpha": 0.5, "max_hops": 2, "decay": 0.8,
	})["count"].(float64)), 1)

	// 13. geo (radius)
	assert.GreaterOrEqual(t, int(postJSON("geo", "/api/v1/datasets/all_modes/geo-search", map[string]any{
		"search_type": "radius", "center": map[string]float64{"lat": 37.7749, "lon": -122.4194}, "radius_km": 5.0, "k": 4,
	})["count"].(float64)), 1)

	// 14. geo_box
	assert.GreaterOrEqual(t, int(postJSON("geo_box", "/api/v1/datasets/all_modes/geo-search", map[string]any{
		"search_type": "box", "box": map[string]float64{"min_lat": 37.7, "max_lat": 37.8, "min_lon": -122.5, "max_lon": -122.4}, "k": 4,
	})["count"].(float64)), 1)

	// 15. geo_hybrid
	assert.GreaterOrEqual(t, int(postJSON("geo_hybrid", "/api/v1/datasets/all_modes/geo-search", map[string]any{
		"search_type": "hybrid", "center": map[string]float64{"lat": 37.7749, "lon": -122.4194}, "radius_km": 5.0, "query_vector": []float32{1, 0, 0, 0}, "k": 4,
	})["count"].(float64)), 1)

	// 16. temporal (as_of)
	assert.GreaterOrEqual(t, int(postJSON("temporal", "/api/v1/datasets/all_modes/temporal-search", map[string]any{
		"search_type": "as_of", "timestamp": baseTs, "k": 4,
	})["count"].(float64)), 1)

	// 17. temporal_range
	assert.GreaterOrEqual(t, int(postJSON("temporal_range", "/api/v1/datasets/all_modes/temporal-search", map[string]any{
		"search_type": "range", "start_time": baseTs - 400, "end_time": baseTs, "k": 4,
	})["count"].(float64)), 1)

	// 18. temporal_window
	assert.GreaterOrEqual(t, int(postJSON("temporal_window", "/api/v1/datasets/all_modes/temporal-search", map[string]any{
		"search_type": "sliding_window", "window_size": 2, "k": 4,
	})["count"].(float64)), 1)

	// 19. multimodal
	_, err := vs.GetMultiModalPipeline().IngestItems(ctx, "mm_all_modes", []MultiModalItem{
		{ID: "mm-0", Modality: ModalityText, Caption: "quantum encryption"},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mmds, ok := vs.getDataset("mm_all_modes")
		return ok && mmds.IndexLen() >= 1
	}, 5*time.Second, 25*time.Millisecond)
	assert.GreaterOrEqual(t, int(postJSON("multimodal", "/api/v1/datasets/mm_all_modes/multimodal-search", map[string]any{
		"modality": "text", "text_query": "quantum", "k": 1,
	})["count"].(float64)), 1)

	// 20. federated
	assert.GreaterOrEqual(t, int(postJSON("federated", "/api/v1/search/federated", map[string]any{
		"collections": []string{"all_modes"}, "vector": []float32{1, 0, 0, 0}, "k": 3, "rrf_k": 60,
	})["count"].(float64)), 1)

	// 21. learned_index
	doGetCount("learned_index", query.TicketQuery{Search: &query.VectorSearchRequest{
		Dataset: "all_modes", Vector: []float32{1, 0, 0, 0}, K: 3, EnableLearnedIndex: true,
	}})
}
