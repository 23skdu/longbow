package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/23skdu/longbow/pkg/wasmclient"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestHTTPHandlersAndEdgeClientRoundtrip(t *testing.T) {
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

	// Seed dataset via Edge Client offline queue + EdgeSyncHTTPHandler
	ec, err := wasmclient.NewEdgeClient(wasmclient.ClientConfig{
		Endpoint:     srv.URL,
		Dataset:      "edge_ds",
		OfflineFirst: true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, ec.Upsert(ctx, wasmclient.EdgeDocument{
		ID:       "d1",
		Vector:   []float32{1, 0, 0, 0},
		Metadata: map[string]any{"env": "edge"},
	}))
	require.NoError(t, ec.Upsert(ctx, wasmclient.EdgeDocument{
		ID:       "d2",
		Vector:   []float32{0, 1, 0, 0},
		Metadata: map[string]any{"env": "edge"},
	}))
	require.NoError(t, ec.FlushSync(ctx))

	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("edge_ds")
		return ok && ds.IndexLen() >= 2
	}, 2*time.Second, 20*time.Millisecond)

	// Remote search via EdgeSearchHTTPHandler (/api/v1/search)
	ecRemote, err := wasmclient.NewEdgeClient(wasmclient.ClientConfig{
		Endpoint:     srv.URL,
		Dataset:      "edge_ds",
		OfflineFirst: false,
	})
	require.NoError(t, err)
	res, err := ecRemote.Search(ctx, []float32{1, 0, 0, 0}, 2)
	require.NoError(t, err)
	require.NotEmpty(t, res)

	// Verify /api/v1/usage and /api/v1/usage/export HTTP endpoints
	resp, err := srv.Client().Get(srv.URL + "/api/v1/usage")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	expResp, err := srv.Client().Get(srv.URL + "/api/v1/usage/export?format=csv")
	require.NoError(t, err)
	defer func() { _ = expResp.Body.Close() }()
	require.Equal(t, http.StatusOK, expResp.StatusCode)
	require.Contains(t, expResp.Header.Get("Content-Type"), "text/csv")

	// Verify query quota enforcement blocks once hard limit is exceeded
	um := vs.GetUsageMeter()
	um.SetQuota("default", TenantQuota{
		EnforceHardLimits:   true,
		MaxQueriesPerWindow: 1,
		WindowSeconds:       3600,
	})
	_ = um.CheckAndConsumeQuery(ctx, "default", 128)
	dec := um.CheckAndConsumeQuery(ctx, "default", 128)
	require.False(t, dec.Allowed)
	require.Equal(t, "queries", dec.Resource)
}

func TestDoActionFeatureDispatchEndToEnd(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	rec := buildRichBatch(t, mem, []struct {
		id       string
		category string
		price    float64
		text     string
		vec      []float32
		metaJSON string
	}{
		{"a1", "docs", 12.0, "apache arrow vector database", []float32{1, 0, 0, 0}, `{"category":"docs"}`},
	})
	defer rec.Release()
	ctx := context.Background()
	require.NoError(t, vs.StoreRecordBatch(ctx, "feat_ds", rec))
	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("feat_ds")
		return ok && ds.IndexLen() >= 1
	}, 2*time.Second, 20*time.Millisecond)

	callAction := func(actionType string, body any) map[string]any {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		stream := &captureActionStream{ctx: ctx}
		require.NoError(t, vs.DoAction(&flight.Action{Type: actionType, Body: raw}, stream))
		require.NotEmpty(t, stream.results, "expected result for action %s", actionType)
		var out map[string]any
		require.NoError(t, json.Unmarshal(stream.results[0].Body, &out))
		return out
	}

	rw := callAction("RewriteQuery", map[string]any{"query": "vector database", "mode": "synonyms"})
	require.NotEmpty(t, rw["queries"])

	rr := callAction("Rerank", map[string]any{
		"query":    "vector database",
		"strategy": "hybrid",
		"top_k":    1,
		"results": []map[string]any{
			{"id": 1, "score": 0.9},
		},
	})
	require.NotEmpty(t, rr["results"])

	as := callAction("GetAutoScaleConfig", map[string]any{})
	_, hasMin := as["min_shards"]
	require.True(t, hasMin)

	ts := callAction("AdvancedTextSearch", map[string]any{"dataset": "feat_ds", "query": "arrow", "k": 5})
	require.EqualValues(t, 1, ts["total_hits"])

	exp := callAction("ExplainSearch", map[string]any{"dataset": "feat_ds", "vector": []float32{1, 0, 0, 0}, "k": 1})
	require.Equal(t, "feat_ds", exp["dataset"])

	usage := callAction("GetTenantUsage", map[string]any{"tenant_id": "default"})
	require.Equal(t, "default", usage["tenant_id"])
}
