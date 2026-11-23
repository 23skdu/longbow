package engine

import (
	"net/http"

	"github.com/23skdu/longbow/internal/store/apihandlers"
)

// EdgeStreamManager exposes the streaming manager through the narrow interface
// consumed by the API handler domain.
func (s *VectorStore) EdgeStreamManager() apihandlers.StreamManagerHost {
	return s.GetStreamManager()
}

// EdgeSearchHTTPHandler provides HTTP and gRPC-Web search transport.
func (s *VectorStore) EdgeSearchHTTPHandler() http.Handler {
	return apihandlers.EdgeSearchHTTPHandler(s)
}

// EdgeSyncHTTPHandler accepts batched offline-first edge mutations.
func (s *VectorStore) EdgeSyncHTTPHandler() http.Handler {
	return apihandlers.EdgeSyncHTTPHandler(s)
}

var (
	_ apihandlers.Host = (*VectorStore)(nil)
)
