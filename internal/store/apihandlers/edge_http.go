package apihandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/security"
	"github.com/23skdu/longbow/internal/store/rbac"
	"github.com/23skdu/longbow/internal/store/streaming"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/pkg/wasmclient"
)

// StreamManagerHost is the narrow streaming capability used by edge handlers.
// It avoids depending on the concrete store or connector manager types.
type StreamManagerHost interface {
	GetConnector(name string) (*streaming.StreamConnector, bool)
	CreateConnector(cfg streaming.StreamConnectorConfig, startNow bool) (*streaming.StreamConnector, error)
}

// Host is the store capability surface required by API handlers. It contains
// no *VectorStore or *Dataset references, so the API domain cannot create an
// import cycle with the store engine.
type Host interface {
	AuthorizeDatasetAccess(context.Context, rbac.Permission, string) (*security.TenantContext, error)
	SearchDatasetVectors(context.Context, string, []float32, int, []core.Filter) ([]types.SearchResult, error)
	EdgeStreamManager() StreamManagerHost
}

// EdgeSearchHTTPHandler provides HTTP and gRPC-Web search transport for WASM, Browser,
// Cloudflare Workers, and Deno clients without requiring native HTTP/2 gRPC.
func EdgeSearchHTTPHandler(host Host) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		isGRPCWeb := strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc-web") || r.Header.Get("X-Grpc-Web") == "1"
		if isGRPCWeb && len(rawBody) >= 5 && rawBody[0] == 0x00 {
			unframed, _, fErr := wasmclient.DecodeGRPCWebFrame(rawBody)
			if fErr != nil {
				http.Error(w, fErr.Error(), http.StatusBadRequest)
				return
			}
			rawBody = unframed
		}
		var req struct {
			Dataset string    `json:"dataset"`
			Vector  []float32 `json:"vector"`
			K       int       `json:"k"`
		}
		if err := json.Unmarshal(rawBody, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := host.AuthorizeDatasetAccess(r.Context(), PermRead, req.Dataset); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		if req.K <= 0 {
			req.K = 10
		}
		res, err := host.SearchDatasetVectors(r.Context(), req.Dataset, req.Vector, req.K, nil)
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
		respJSON, _ := json.Marshal(map[string]any{"results": edgeRes})
		if isGRPCWeb {
			w.Header().Set("Content-Type", "application/grpc-web+json")
			_, _ = w.Write(wasmclient.EncodeGRPCWebFrame(respJSON, true))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(respJSON)
	})
}

// EdgeSyncHTTPHandler accepts batched offline-first edge mutations and returns conflict-free state.
func EdgeSyncHTTPHandler(host Host) http.Handler {
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
		if _, err := host.AuthorizeDatasetAccess(r.Context(), PermWrite, req.Dataset); err != nil {
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
			conn, exists := host.EdgeStreamManager().GetConnector(connName)
			if !exists {
				var err error
				conn, err = host.EdgeStreamManager().CreateConnector(StreamConnectorConfig{
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
				if err := conn.IngestEvents(r.Context(), events); err != nil {
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
