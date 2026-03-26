package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

// RESTServer exposes the internal VectorStore via HTTP/JSON
type RESTServer struct {
	vectorStore *store.VectorStore
	logger      *zerolog.Logger
	server      *http.Server
	addr        string
	mu          sync.Mutex
}

func NewRESTServer(addr string, vs *store.VectorStore, logger *zerolog.Logger) *RESTServer {
	return &RESTServer{
		addr:        addr,
		vectorStore: vs,
		logger:      logger,
	}
}

func (s *RESTServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/datasets", s.handleGetDatasets)
	mux.HandleFunc("/v1/search", s.handleSearch)
	mux.HandleFunc("/v1/upsert", s.handleUpsert)

	s.server = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	s.logger.Info().Str("addr", s.addr).Msg("Starting REST gateway")
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error().Err(err).Msg("REST server failed")
		}
	}()

	return nil
}

func (s *RESTServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *RESTServer) handleGetDatasets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var datasets []string
	s.vectorStore.IterateDatasets(func(name string, ds *store.Dataset) {
		datasets = append(datasets, name)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"datasets": datasets,
	})
}

func (s *RESTServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req query.VectorSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.K < 1 {
		req.K = 10
	}

	var queryVectors [][]float32
	if len(req.Vector) > 0 {
		queryVectors = append(queryVectors, req.Vector)
	}
	if len(req.Vectors) > 0 {
		queryVectors = append(queryVectors, req.Vectors...)
	}

	if len(queryVectors) == 0 && req.TextQuery == "" {
		http.Error(w, "no query vectors or text_query provided", http.StatusBadRequest)
		return
	}

	// Map to internal hybrid search parameters
	alpha := req.Alpha
	if alpha == 0 && req.TextQuery == "" && len(queryVectors) > 0 {
		alpha = 1.0 // Default to pure dense if no text query + vector present
	}

	var allResults [][]types.SearchResult
	for _, qVec := range queryVectors {
		results, err := s.vectorStore.SearchHybrid(
			r.Context(),
			req.Dataset,
			qVec,
			req.TextQuery,
			req.K,
			alpha,
			60, // rrfK
			req.GraphAlpha,
			2, // graphDepth
		)
		if err != nil {
			http.Error(w, fmt.Sprintf("Search failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Hydrate with mapped user IDs if available
		ds, err := s.vectorStore.GetDataset(req.Dataset)
		if err == nil && ds != nil {
			results = s.vectorStore.MapInternalToUserIDs(ds, results)
		}

		allResults = append(allResults, results)
	}

	w.Header().Set("Content-Type", "application/json")
	if len(queryVectors) == 1 {
		// Flatten response for single query
		json.NewEncoder(w).Encode(map[string]interface{}{
			"results": allResults[0],
		})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"batch_results": allResults,
		})
	}
}

func (s *RESTServer) handleUpsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Because building an Arrow RecordBatch directly from untyped REST JSON involves complex schema mapping,
	// direct translation is pending schema definition standardizations. Users should use gRPC for high-performance Arrow IPC ingest.
	http.Error(w, "501 Not Implemented: Upsert via REST is pending native builder availability. Please use Arrow Flight gRPC for data ingest.", http.StatusNotImplemented)
}
