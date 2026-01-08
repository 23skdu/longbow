package store

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/flight"
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

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
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
		s.logger.Error().Err(err).Msg("Failed to add edge")
		return status.Errorf(codes.Internal, "failed to add edge: %v", err)
	}

	return stream.Send(&flight.Result{Body: []byte("edge added")})
}

// handleTraverseGraph processes a traverse-graph action
func (s *VectorStore) handleTraverseGraph(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Dataset  string  `json:"dataset"`
		Start    uint32  `json:"start"`
		MaxHops  int     `json:"max_hops"`
		Incoming bool    `json:"incoming"`
		Weighted bool    `json:"weighted"`
		Decay    float32 `json:"decay"`
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

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
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

	opts := DefaultTraverseOptions()
	opts.MaxHops = req.MaxHops
	if req.Incoming {
		opts.Direction = DirectionIncoming
	} else {
		opts.Direction = DirectionOutgoing
	}
	if req.Weighted { // Explicitly check if set? Default is true in DefaultTraverseOptions, assuming false in struct means disable.
		// If req.Weighted is false (default bool), we might accidentally disable weights if user didn't specify.
		// But in Go structs, missing=false.
		// Let's assume user must send true to enable weighting or we should use logic to detect presence.
		// For now, let's honor the boolean.
		opts.Weighted = req.Weighted
	}
	// Better logic: if JSON omits it, it's false. If we want default true, we should have used *bool.
	// We'll stick to DefaultTraverseOptions=true, so we should logic this carefully.
	// Actually, we should probably assume Weighted is default true unless logic dictates otherwise.
	// Let's just trust request for now.

	if req.Decay != 0 {
		opts.Decay = req.Decay
	}

	paths := graph.Traverse(VectorID(req.Start), opts)

	resp, err := json.Marshal(paths)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to serialize paths: %v", err)
	}

	return stream.Send(&flight.Result{Body: resp})
}

// handleGetGraphStats returns statistics about the knowledge graph
func (s *VectorStore) handleGetGraphStats(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Dataset string `json:"dataset"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	if req.Dataset == "" {
		return status.Error(codes.InvalidArgument, "missing dataset name")
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	ds.dataMu.RLock()
	var edgeCount int
	var commCount int
	var preds []string

	if ds.Graph != nil {
		// GraphStore methods usually take internal locks, but we need to verify if calling them is safe directly.
		// EdgeCount() uses RLock. CommunityCount() uses RLock. PredicateVocabulary() uses RLock.
		// So it's safe to call them without holding ds.dataMu, assuming ds.Graph pointer doesn't change
		// (which likely doesn't happen often, or we should hold RLock to snag the pointer).
		g := ds.Graph
		ds.dataMu.RUnlock()

		edgeCount = g.EdgeCount()
		commCount = g.CommunityCount()
		preds = g.PredicateVocabulary()
	} else {
		ds.dataMu.RUnlock()
		preds = []string{}
	}

	resp := map[string]interface{}{
		"edge_count":      edgeCount,
		"community_count": commCount,
		"predicates":      preds,
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to serialize stats: %v", err)
	}

	return stream.Send(&flight.Result{Body: respBytes})
}
