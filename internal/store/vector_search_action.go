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

// VectorSearchRequest defines the request format for VectorSearch action
type VectorSearchRequest struct {
Dataset string    `json:"dataset"`
Vector  []float32 `json:"vector"`
K       int       `json:"k"`
}

// VectorSearchResponse defines the response format for VectorSearch action
type VectorSearchResponse struct {
IDs    []uint64  `json:"ids"`
Scores []float32 `json:"scores"`
}

// handleVectorSearchAction handles the VectorSearch DoAction request
func (s *MetaServer) handleVectorSearchAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
start := time.Now()

// Parse request
var req VectorSearchRequest
if err := json.Unmarshal(action.Body, &req); err != nil {
metrics.VectorSearchActionErrors.Inc()
return status.Errorf(codes.InvalidArgument, "invalid JSON request: %v", err)
}

// Validate K
if req.K < 1 {
metrics.VectorSearchActionErrors.Inc()
return status.Error(codes.InvalidArgument, "k must be at least 1")
}

// Get dataset
ds, ok := s.vectors.Get(req.Dataset)
if !ok {
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
if uint32(len(req.Vector)) != expectedDim {
metrics.VectorSearchActionErrors.Inc()
return status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", expectedDim, len(req.Vector))
}

// Perform search using arena pool for zero-copy
arena := GetArena()
defer PutArena(arena)

neighbors := ds.Index.SearchWithArena(req.Vector, req.K, arena)

// Build response
resp := VectorSearchResponse{
IDs:    make([]uint64, len(neighbors)),
Scores: make([]float32, len(neighbors)),
}

// TODO: Compute actual distances/scores if needed
for i, id := range neighbors {
resp.IDs[i] = uint64(id)
resp.Scores[i] = float32(i) // Placeholder - actual distance computation can be added
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
zap.Int("results", len(neighbors)),
zap.Duration("duration", time.Since(start)),
)

return nil
}
