package wasmclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEdgeIndexSearchExportAndOfflineSync(t *testing.T) {
	ctx := context.Background()
	syncedBatches := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/edge/sync":
			syncedBatches++
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"remote_docs": []EdgeDocument{
					{ID: "remote-1", Vector: []float32{0, 1, 0, 0}, UpdatedAt: 9999999999},
				},
			})
		case "/api/v1/search":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []EdgeSearchResult{{ID: "remote-hit", Distance: 0.01, Score: 0.99}},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client, err := NewEdgeClient(ClientConfig{
		Endpoint:         srv.URL,
		Dataset:          "edge-notes",
		ClientID:         "browser-1",
		OfflineFirst:     true,
		ConflictStrategy: ConflictVectorClock,
	})
	require.NoError(t, err)

	require.NoError(t, client.Upsert(ctx, EdgeDocument{
		ID:       "doc-a",
		Vector:   []float32{1, 0, 0, 0},
		Metadata: map[string]any{"tag": "auth"},
	}))
	require.NoError(t, client.Upsert(ctx, EdgeDocument{
		ID:       "doc-b",
		Vector:   []float32{0.8, 0.2, 0, 0},
		Metadata: map[string]any{"tag": "vector"},
	}))
	require.Equal(t, 2, client.SyncEngine().PendingCount())

	// Offline HNSW query works before sync
	hits, err := client.Search(ctx, []float32{1, 0, 0, 0}, 1)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "doc-a", hits[0].ID)

	// Export + re-import local index persistence
	rawSnap, err := client.LocalIndex().ExportJSON()
	require.NoError(t, err)
	restored := NewEdgeIndex(MetricCosine, 16, 64)
	require.NoError(t, restored.ImportJSON(rawSnap))
	require.Equal(t, 2, restored.Len())

	// Flush offline queue to upstream & pull remote doc
	require.NoError(t, client.FlushSync(ctx))
	require.Equal(t, 1, syncedBatches)
	require.Equal(t, 0, client.SyncEngine().PendingCount())
	require.Equal(t, 3, client.LocalIndex().Len())

	// Vector clock conflict resolution check
	se := NewSyncEngine("edge-a", ConflictVectorClock)
	winner, merged := se.ResolveConflict(
		EdgeDocument{ID: "x", UpdatedAt: 100},
		map[string]int64{"edge-a": 2},
		EdgeDocument{ID: "x", UpdatedAt: 200},
		map[string]int64{"edge-a": 2, "edge-b": 1},
		"edge-b",
	)
	require.Equal(t, int64(200), winner.UpdatedAt)
	require.Equal(t, int64(1), merged["edge-b"])

	// Arrow columnar binary serialization round-trip
	colRaw, err := EncodeArrowColumnarDocs([]EdgeDocument{
		{ID: "c1", Vector: []float32{0.1, 0.9, 0.0, 0.0}, UpdatedAt: 111},
		{ID: "c2", Vector: []float32{0.8, 0.2, 0.0, 0.0}, UpdatedAt: 222},
	})
	require.NoError(t, err)
	decodedDocs, err := DecodeArrowColumnarDocs(colRaw)
	require.NoError(t, err)
	require.Len(t, decodedDocs, 2)
	require.Equal(t, "c1", decodedDocs[0].ID)
	require.InDelta(t, 0.9, decodedDocs[0].Vector[1], 1e-6)

	// gRPC-Web binary framing round-trip
	grpcPayload := []byte(`{"results":[{"id":"gw-1","distance":0.01,"score":0.99}]}`)
	framed := EncodeGRPCWebFrame(grpcPayload, true)
	unframed, trailer, err := DecodeGRPCWebFrame(framed)
	require.NoError(t, err)
	require.Equal(t, grpcPayload, unframed)
	require.Contains(t, trailer, "grpc-status: 0")
}

func FuzzEdgeIndexAndSync(f *testing.F) {
	f.Add("id-1", float32(0.5), float32(0.5), int64(100))
	f.Add("id-2", float32(-1.0), float32(0.0), int64(200))

	f.Fuzz(func(t *testing.T, id string, x, y float32, ts int64) {
		idx := NewEdgeIndex(MetricCosine, 8, 16)
		se := NewSyncEngine("c1", ConflictVectorClock)
		doc := EdgeDocument{ID: id, Vector: []float32{x, y}, UpdatedAt: ts}
		_ = idx.Upsert(doc)
		if id != "" {
			se.RecordLocal("upsert", doc, ts)
			_, _ = idx.Search([]float32{x, y}, 2)
		}
	})
}
