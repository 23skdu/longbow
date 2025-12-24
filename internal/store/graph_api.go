package store

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handleAddEdge processes an add-edge action
func (s *VectorStore) handleAddEdge(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Dataset   string  `json:"dataset"`
		Subject   uint32  `json:"subject"`
		Predicate string  `json:"predicate"`
		Object    uint32  `json:"object"`
		Weight    float32 `json:"weight"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	if req.Dataset == "" {
		return status.Error(codes.InvalidArgument, "missing dataset name")
	}
	if req.Predicate == "" {
		return status.Error(codes.InvalidArgument, "missing predicate")
	}

	ds, err := s.getDataset(req.Dataset)
	if err != nil {
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	// Ensure GraphStore exists
	ds.dataMu.Lock()
	if ds.Graph == nil {
		ds.Graph = NewGraphStore()
	}
	ds.dataMu.Unlock()

	edge := Edge{
		Subject:   VectorID(req.Subject),
		Predicate: req.Predicate,
		Object:    VectorID(req.Object),
		Weight:    req.Weight,
	}

	if err := ds.Graph.AddEdge(edge); err != nil {
		s.logger.Error("Failed to add edge", zap.Error(err))
		return status.Errorf(codes.Internal, "failed to add edge: %v", err)
	}

	return stream.Send(&flight.Result{Body: []byte("edge added")})
}

// handleTraverseGraph processes a traverse-graph action
func (s *VectorStore) handleTraverseGraph(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Dataset string `json:"dataset"`
		Start   uint32 `json:"start"`
		MaxHops int    `json:"max_hops"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	if req.Dataset == "" {
		return status.Error(codes.InvalidArgument, "missing dataset name")
	}
	if req.MaxHops <= 0 {
		req.MaxHops = 2 // Default depth
	}

	ds, err := s.getDataset(req.Dataset)
	if err != nil {
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	ds.dataMu.RLock()
	if ds.Graph == nil || ds.Graph.EdgeCount() == 0 {
		ds.dataMu.RUnlock()
		// Return empty list
		emptyJSON, _ := json.Marshal([]Path{})
		return stream.Send(&flight.Result{Body: emptyJSON})
	}
	graph := ds.Graph
	ds.dataMu.RUnlock()

	paths := graph.Traverse(VectorID(req.Start), req.MaxHops)

	resp, err := json.Marshal(paths)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to serialize paths: %v", err)
	}

	return stream.Send(&flight.Result{Body: resp})
}
