package store

import (
	"encoding/json"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Global zero-alloc parser for VectorSearch (reusable, pre-allocated for 768 dims)
var vectorSearchParser = NewZeroAllocVectorSearchParser(768)

// VectorSearchRequest defines the request format for VectorSearch action
type VectorSearchRequest struct {
	Dataset   string      `json:"dataset"`
	Vector    []float32   `json:"vector"`  // Single vector (legacy/simple)
	Vectors   [][]float32 `json:"vectors"` // Multiple vectors for pipelining
	K         int         `json:"k"`
	Filters   []Filter    `json:"filters"`
	LocalOnly bool        `json:"local_only"`
	// Hybrid Search Fields
	TextQuery string  `json:"text_query"`
	Alpha     float32 `json:"alpha"` // 0.0=sparse, 1.0=dense, 0.5=hybrid
}

// VectorSearchResponse defines the response format for VectorSearch action
type VectorSearchResponse struct {
	IDs    []uint64  `json:"ids"`
	Scores []float32 `json:"scores"`
}

// handleVectorSearchAction handles the VectorSearch DoAction request
func (s *MetaServer) handleVectorSearchAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	start := time.Now()

	var req VectorSearchRequest
	var parseErr error
	req, parseErr = vectorSearchParser.Parse(action.Body)
	if parseErr != nil {
		metrics.VectorSearchParseFallbackTotal.Inc()
		req = VectorSearchRequest{}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			s.logger.Warn().Err(err).Msg("VectorSearch JSON parse failed")
			return status.Errorf(codes.InvalidArgument, "invalid JSON request: %v", err)
		}
	} else {
		metrics.ZeroAllocVectorSearchParseTotal.Inc()
	}
	// Validate K
	if req.K < 1 {
		metrics.VectorSearchActionErrors.Inc()
		return status.Error(codes.InvalidArgument, "k must be at least 1")
	}

	// Determine if Hybrid Search
	isHybrid := req.TextQuery != "" || (req.Alpha > 0 && req.Alpha < 1.0)

	// Combine Vector and Vectors into a single slice for processing
	var queryVectors [][]float32
	if len(req.Vector) > 0 {
		queryVectors = append(queryVectors, req.Vector)
	}
	if len(req.Vectors) > 0 {
		queryVectors = append(queryVectors, req.Vectors...)
	}

	if len(queryVectors) == 0 {
		metrics.VectorSearchActionErrors.Inc()
		return status.Error(codes.InvalidArgument, "no query vector(s) provided")
	}
	// Process each query vector
	for _, queryVec := range queryVectors {
		var searchResults []SearchResult
		var err error

		if isHybrid {
			// Perform Hybrid Search
			searchResults, err = s.SearchHybrid(stream.Context(), req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60)
			if err != nil {
				metrics.VectorSearchActionErrors.Inc()
				continue // For pipelining, we might want to continue or return error? Let's return error for now to be safe, or log it.
				// For now, let's stop on first error for simplicity unless user wants full partial results.
			}
		} else {
			// Standard Vector Search
			ds, err := s.getDataset(req.Dataset)
			if err != nil {
				metrics.VectorSearchActionErrors.Inc()
				return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
			}

			// Acquire read lock
			ds.dataMu.RLock()
			if ds.evicting.Load() {
				ds.dataMu.RUnlock()
				metrics.EvictionRejectedQueries.Inc()
				return status.Errorf(codes.Unavailable, "dataset %s is being evicted", req.Dataset)
			}

			if ds.Index == nil {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				return status.Error(codes.FailedPrecondition, "dataset has no HNSW index")
			}

			// Validate dimension
			expectedDim := ds.Index.GetDimension()
			if uint32(len(queryVec)) != expectedDim {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				s.logger.Warn().
					Str("dataset", req.Dataset).
					Uint32("expected", expectedDim).
					Int("got", len(queryVec)).
					Int("index_len", ds.Index.Len()).
					Msg("Dimension mismatch in VectorSearch")
				return status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", expectedDim, len(queryVec))
			}

			// Perform search
			var errSearch error
			searchResults, errSearch = ds.Index.SearchVectors(queryVec, req.K, req.Filters)
			if errSearch != nil {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				return status.Errorf(codes.Internal, "vector search failed: %v", errSearch)
			}

			// Map internal IDs to User IDs
			searchResults = s.MapInternalToUserIDs(ds, searchResults)
			ds.dataMu.RUnlock()
		}

		// Perform Global Search (Scatter-Gather) if not local-only
		if !req.LocalOnly && s.Mesh != nil {
			peers := s.Mesh.GetMembers()
			var remotePeers []mesh.Member
			selfID := s.Mesh.GetIdentity().ID
			for _, p := range peers {
				if p.ID != selfID {
					remotePeers = append(remotePeers, p)
				}
			}

			// Override Vector in request for remote peers
			batchReq := req
			batchReq.Vector = queryVec
			batchReq.Vectors = nil // Don't forward the whole batch to each peer for each call?
			// Wait, the aggregator/coordinator might need adjustment too.
			// For now, let's just forward the single vector.
			searchResults, err = s.coordinator.GlobalSearch(stream.Context(), searchResults, batchReq, remotePeers)
			if err != nil {
				s.logger.Warn().Err(err).Msg("Global search partial failure")
			}
		}

		// Build and send response for this vector
		resp := VectorSearchResponse{
			IDs:    make([]uint64, len(searchResults)),
			Scores: make([]float32, len(searchResults)),
		}
		for i, res := range searchResults {
			resp.IDs[i] = uint64(res.ID)
			resp.Scores[i] = res.Score
		}

		respBytes, _ := json.Marshal(resp)
		if err := stream.Send(&flight.Result{Body: respBytes}); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			return err
		}
	}

	metrics.VectorSearchActionTotal.Inc()
	metrics.VectorSearchActionDuration.Observe(time.Since(start).Seconds())
	return nil
}
