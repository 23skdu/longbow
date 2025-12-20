package store

import (
	"encoding/json"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Global zero-alloc parser for VectorSearch (reusable, pre-allocated for 768 dims)
var vectorSearchParser = NewZeroAllocVectorSearchParser(768)

// VectorSearchRequest defines the request format for VectorSearch action
type VectorSearchRequest struct {
	Dataset string    `json:"dataset"`
	Vector  []float32 `json:"vector"`
	K       int       `json:"k"`
	Filters []Filter  `json:"filters"`
}

// VectorSearchResponse defines the response format for VectorSearch action
type VectorSearchResponse struct {
	IDs    []uint64  `json:"ids"`
	Scores []float32 `json:"scores"`
}

// handleVectorSearchAction handles the VectorSearch DoAction request
func (s *MetaServer) handleVectorSearchAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	start := time.Now()

	// Parse request using zero-alloc parser with fallback
	var req VectorSearchRequest
	var parseErr error
	req, parseErr = vectorSearchParser.Parse(action.Body)
	if parseErr != nil {
		// Fallback to standard JSON parser for edge cases
		metrics.VectorSearchParseFallbackTotal.Inc()
		if err := json.Unmarshal(action.Body, &req); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			s.logger.Warn("VectorSearch JSON parse failed", zap.Error(err))
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

	// Get dataset
	ds, err := s.getDataset(req.Dataset)
	if err != nil {
		metrics.VectorSearchActionErrors.Inc()
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	// Check if index exists
	if ds.Index == nil {
		metrics.VectorSearchActionErrors.Inc()
		return status.Error(codes.FailedPrecondition, "dataset has no HNSW index")
	}

	// Validate vector dimension
	expectedDim := ds.Index.GetDimension()
	if uint32(len(req.Vector)) != expectedDim { //nolint:gosec // G115 - vector length unlikely to exceed uint32
		metrics.VectorSearchActionErrors.Inc()
		return status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", expectedDim, len(req.Vector))
	}

	// Perform search using SearchVectors from Index interface
	searchResults := ds.Index.SearchVectors(req.Vector, req.K, req.Filters)

	// Build response
	resp := VectorSearchResponse{
		IDs:    make([]uint64, len(searchResults)),
		Scores: make([]float32, len(searchResults)),
	}

	for i, res := range searchResults {
		resp.IDs[i] = uint64(res.ID)
		resp.Scores[i] = res.Score
	}

	// Serialize response
	respBytes, err := json.Marshal(resp)
	if err != nil {
		metrics.VectorSearchActionErrors.Inc()
		return status.Errorf(codes.Internal, "failed to serialize response: %v", err)
	}

	// Send response
	if err := stream.Send(&flight.Result{Body: respBytes}); err != nil {
		metrics.VectorSearchActionErrors.Inc()
		return err
	}

	// Record metrics
	metrics.VectorSearchActionTotal.Inc()
	metrics.VectorSearchActionDuration.Observe(time.Since(start).Seconds())

	s.logger.Debug("VectorSearch completed",
		zap.String("dataset", req.Dataset),
		zap.Int("k", req.K),
		zap.Int("results", len(searchResults)),
		zap.Duration("duration", time.Since(start)),
	)

	return nil
}
