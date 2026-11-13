package store

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/23skdu/longbow/pkg/wasmclient"
)

// EdgeSearchHTTPHandler provides HTTP/edge search transport for WASM, Browser,
// Cloudflare Workers, and Deno clients without requiring native gRPC.
func (s *VectorStore) EdgeSearchHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
		var req struct {
			Dataset string    `json:"dataset"`
			Vector  []float32 `json:"vector"`
			K       int       `json:"k"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermRead, req.Dataset); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		res, err := s.SearchDatasetVectors(r.Context(), req.Dataset, req.Vector, req.K, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		edgeRes := make([]wasmclient.EdgeSearchResult, len(res))
		for i, item := range res {
			edgeRes[i] = wasmclient.EdgeSearchResult{
				ID:       fmt.Sprint(item.ID),
				Distance: item.Distance,
				Score:    item.Score,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": edgeRes})
	})
}

// EdgeSyncHTTPHandler accepts batched offline-first edge mutations and returns conflict-free state.
func (s *VectorStore) EdgeSyncHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
		var req struct {
			Dataset   string                       `json:"dataset"`
			ClientID  string                       `json:"client_id"`
			Mutations []wasmclient.PendingMutation `json:"mutations"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := s.AuthorizeDatasetAccess(r.Context(), PermWrite, req.Dataset); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		events := make([]StreamVectorEvent, 0, len(req.Mutations))
		for _, m := range req.Mutations {
			events = append(events, StreamVectorEvent{
				Op:            m.Op,
				ID:            m.Doc.ID,
				Vector:        m.Doc.Vector,
				Metadata:      m.Doc.Metadata,
				TimestampUnix: m.Timestamp,
			})
		}
		applied := 0
		if len(events) > 0 {
			connName := "__edge_sync_" + req.Dataset
			conn, exists := s.GetStreamManager().GetConnector(connName)
			if !exists {
				var err error
				conn, err = s.GetStreamManager().CreateConnector(StreamConnectorConfig{
					Name:       connName,
					Dataset:    req.Dataset,
					SourceType: StreamSourceMemory,
				}, true)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
			if conn != nil {
				if err := conn.ingestEvents(r.Context(), events); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				applied = len(events)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"applied": applied, "remote_docs": []any{}})
	})
}
