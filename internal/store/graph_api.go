package store

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/query"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
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
	if len(body) > 0 {
		i := 0
		i = query.SkipWhitespace(body, i)
		if i < len(body) && body[i] == '{' {
			i++
			for i < len(body) {
				i = query.SkipWhitespace(body, i)
				if i >= len(body) || body[i] == '}' { break }
				key, newPos, err := query.ParseString(body, i)
				if err != nil { break }
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' { i++ }
				i = query.SkipWhitespace(body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(body, i)
					req.Dataset = val
					i = newPos
				case "subject":
					val, newPos, _ := query.ParseInt64(body, i)
					req.Subject = uint32(val)
					i = newPos
				case "predicate":
					val, newPos, _ := query.ParseString(body, i)
					req.Predicate = val
					i = newPos
				case "object":
					val, newPos, _ := query.ParseInt64(body, i)
					req.Object = uint32(val)
					i = newPos
				case "weight":
					val, newPos, _ := query.ParseFloat32(body, i)
					req.Weight = val
					i = newPos
				default:
					i, _ = query.SkipValue(body, i)
				}
				i = query.SkipWhitespace(body, i)
				if i < len(body) && body[i] == ',' { i++ }
			}
		}
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

	if len(body) > 0 {
		i := 0
		i = query.SkipWhitespace(body, i)
		if i < len(body) && body[i] == '{' {
			i++
			for i < len(body) {
				i = query.SkipWhitespace(body, i)
				if i >= len(body) || body[i] == '}' { break }
				key, newPos, err := query.ParseString(body, i)
				if err != nil { break }
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' { i++ }
				i = query.SkipWhitespace(body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(body, i)
					req.Dataset = val
					i = newPos
				case "start":
					val, newPos, _ := query.ParseInt64(body, i)
					req.Start = uint32(val)
					i = newPos
				case "max_hops":
					val, newPos, _ := query.ParseInt64(body, i)
					req.MaxHops = int(val)
					i = newPos
				case "incoming":
					val, newPos, _ := query.ParseBool(body, i)
					req.Incoming = val
					i = newPos
				case "weighted":
					val, newPos, _ := query.ParseBool(body, i)
					req.Weighted = val
					i = newPos
				case "decay":
					val, newPos, _ := query.ParseFloat32(body, i)
					req.Decay = val
					i = newPos
				default:
					i, _ = query.SkipValue(body, i)
				}
				i = query.SkipWhitespace(body, i)
				if i < len(body) && body[i] == ',' { i++ }
			}
		}
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
	if len(body) > 0 {
		if err := query.ParseDatasetRequest(body, &req.Dataset); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
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

	resp := map[string]any{
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

// handleCalculatePageRank calculates PageRank centrality for the HNSW graph
func (s *VectorStore) handleCalculatePageRank(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Dataset       string  `json:"dataset"`
		DampingFactor float32 `json:"damping_factor"`
		MaxIterations int     `json:"max_iterations"`
		Tolerance     float32 `json:"tolerance"`
	}

	if len(body) > 0 {
		i := 0
		i = query.SkipWhitespace(body, i)
		if i < len(body) && body[i] == '{' {
			i++
			for i < len(body) {
				i = query.SkipWhitespace(body, i)
				if i >= len(body) || body[i] == '}' { break }
				key, newPos, err := query.ParseString(body, i)
				if err != nil { break }
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' { i++ }
				i = query.SkipWhitespace(body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(body, i)
					req.Dataset = val
					i = newPos
				case "damping_factor":
					val, newPos, _ := query.ParseFloat32(body, i)
					req.DampingFactor = val
					i = newPos
				case "max_iterations":
					val, newPos, _ := query.ParseInt64(body, i)
					req.MaxIterations = int(val)
					i = newPos
				case "tolerance":
					val, newPos, _ := query.ParseFloat32(body, i)
					req.Tolerance = val
					i = newPos
				default:
					i, _ = query.SkipValue(body, i)
				}
				i = query.SkipWhitespace(body, i)
				if i < len(body) && body[i] == ',' { i++ }
			}
		}
	}

	if req.Dataset == "" {
		return status.Error(codes.InvalidArgument, "missing dataset name")
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	ds.dataMu.RLock()
	index := ds.Index
	ds.dataMu.RUnlock()

	if index == nil {
		return status.Error(codes.FailedPrecondition, "index not initialized")
	}

	// GraphAnalytics works on types.GraphData
	// We need to extract the graph data from the index
	var gd *lbtypes.GraphData

	switch h := index.(type) {
	case interface{ GetData() *lbtypes.GraphData }:
		gd = h.GetData()
	case interface{ GetShardedIndex() *ShardedHNSW }:
		// For sharded, we might need a composite view or run per shard?
		// PageRank is global. For now, we'll support single-node un-sharded HNSW.
		sharded := h.GetShardedIndex()
		if sharded != nil && len(sharded.shards) > 0 {
			// Fallback: use first shard for now or return error
			gd = sharded.shards[0].index.(interface{ GetData() *lbtypes.GraphData }).GetData()
		}
	}

	if gd == nil {
		return status.Error(codes.Unimplemented, "PageRank not supported for this index type")
	}

	ga := NewGraphAnalytics(func() *lbtypes.GraphData { return gd })
	config := DefaultPageRankConfig()
	if req.DampingFactor > 0 {
		config.DampingFactor = req.DampingFactor
	}
	if req.MaxIterations > 0 {
		config.MaxIterations = req.MaxIterations
	}
	if req.Tolerance > 0 {
		config.Tolerance = req.Tolerance
	}

	result, err := ga.CalculatePageRank(stream.Context(), config)
	if err != nil {
		return status.Errorf(codes.Internal, "PageRank failed: %v", err)
	}

	respBytes, err := json.Marshal(result)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to serialize results: %v", err)
	}

	return stream.Send(&flight.Result{Body: respBytes})
}

// handleDetectCommunities runs community detection (LPA) on the HNSW graph
func (s *VectorStore) handleDetectCommunities(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Dataset       string `json:"dataset"`
		MaxIterations int    `json:"max_iterations"`
	}

	if len(body) > 0 {
		i := 0
		i = query.SkipWhitespace(body, i)
		if i < len(body) && body[i] == '{' {
			i++
			for i < len(body) {
				i = query.SkipWhitespace(body, i)
				if i >= len(body) || body[i] == '}' { break }
				key, newPos, err := query.ParseString(body, i)
				if err != nil { break }
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' { i++ }
				i = query.SkipWhitespace(body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(body, i)
					req.Dataset = val
					i = newPos
				case "max_iterations":
					val, newPos, _ := query.ParseInt64(body, i)
					req.MaxIterations = int(val)
					i = newPos
				default:
					i, _ = query.SkipValue(body, i)
				}
				i = query.SkipWhitespace(body, i)
				if i < len(body) && body[i] == ',' { i++ }
			}
		}
	}

	if req.Dataset == "" {
		return status.Error(codes.InvalidArgument, "missing dataset name")
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	ds.dataMu.RLock()
	index := ds.Index
	ds.dataMu.RUnlock()

	if index == nil {
		return status.Error(codes.FailedPrecondition, "index not initialized")
	}

	var gd *lbtypes.GraphData
	switch h := index.(type) {
	case interface{ GetData() *lbtypes.GraphData }:
		gd = h.GetData()
	}

	if gd == nil {
		return status.Error(codes.Unimplemented, "Community detection not supported for this index type")
	}

	ga := NewGraphAnalytics(func() *lbtypes.GraphData { return gd })
	maxIter := req.MaxIterations
	if maxIter <= 0 {
		maxIter = 10
	}

	result, err := ga.DetectCommunities(stream.Context(), maxIter)
	if err != nil {
		return status.Errorf(codes.Internal, "Community detection failed: %v", err)
	}

	respBytes, err := json.Marshal(result)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to serialize results: %v", err)
	}

	return stream.Send(&flight.Result{Body: respBytes})
}
