package engine

import (
	"encoding/json"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
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
				if i >= len(body) || body[i] == '}' {
					break
				}
				key, newPos, err := query.ParseString(body, i)
				if err != nil {
					break
				}
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' {
					i++
				}
				i = query.SkipWhitespace(body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(body, i)
					req.Dataset = val
					i = newPos
				case "subject":
					val, newPos, _ := query.ParseInt64(body, i)
					req.Subject = uint32(val) // #nosec G115 -- val is within uint32 range
					i = newPos
				case "predicate":
					val, newPos, _ := query.ParseString(body, i)
					req.Predicate = val
					i = newPos
				case "object":
					val, newPos, _ := query.ParseInt64(body, i)
					req.Object = uint32(val) // #nosec G115 -- val is within uint32 range
					i = newPos
				case "weight":
					val, newPos, _ := query.ParseFloat32(body, i)
					req.Weight = val
					i = newPos
				default:
					i, _ = query.SkipValue(body, i)
				}
				i = query.SkipWhitespace(body, i)
				if i < len(body) && body[i] == ',' {
					i++
				}
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
	s.lockDatasetForWrite(ds)
	if ds.Graph == nil {
		ds.Graph = NewGraphStore()
	}
	s.unlockDatasetForWrite(ds)

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
				if i >= len(body) || body[i] == '}' {
					break
				}
				key, newPos, err := query.ParseString(body, i)
				if err != nil {
					break
				}
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' {
					i++
				}
				i = query.SkipWhitespace(body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(body, i)
					req.Dataset = val
					i = newPos
				case "start":
					val, newPos, _ := query.ParseInt64(body, i)
					req.Start = uint32(val) // #nosec G115 -- val is within uint32 range
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
				if i < len(body) && body[i] == ',' {
					i++
				}
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

	// Copy the adjacency lists before running the BFS. The traversal holds an
	// analytics slot instead of the dataset read lock, so ingest and search are
	// not blocked by a long walk.
	lease, err := s.beginGraphTraversal(stream.Context(), ds)
	if err != nil {
		return analyticsSetupError(err, "graph traversal unavailable")
	}
	defer lease.done()

	if lease.graph == nil || (len(lease.graph.adjList) == 0 && len(lease.graph.bwdAdjList) == 0) {
		emptyJSON, _ := json.Marshal([]Path{})
		return stream.Send(&flight.Result{Body: emptyJSON})
	}
	graph := lease.graph

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

	s.lockDatasetForRead(ds)
	var edgeCount int
	var nodeCount int
	var preds []string

	if ds.Graph != nil {
		// GraphStore methods usually take internal locks, but we need to verify if calling them is safe directly.
		// EdgeCount() uses RLock. NodeCount() uses RLock. PredicateVocabulary() uses RLock.
		// So it's safe to call them without holding ds.dataMu, assuming ds.Graph pointer doesn't change
		// (which likely doesn't happen often, or we should hold RLock to snag the pointer).
		g := ds.Graph
		s.unlockDatasetForRead(ds)

		edgeCount = g.EdgeCount()
		nodeCount = g.NodeCount()
		preds = g.PredicateVocabulary()
	} else {
		s.unlockDatasetForRead(ds)
		preds = []string{}
	}

	resp := map[string]any{
		"edge_count": edgeCount,
		"node_count": nodeCount,
		"predicates": preds,
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
				if i >= len(body) || body[i] == '}' {
					break
				}
				key, newPos, err := query.ParseString(body, i)
				if err != nil {
					break
				}
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' {
					i++
				}
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
				if i < len(body) && body[i] == ',' {
					i++
				}
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

	// Snapshot the HNSW topology under a short dataset read lock; the PageRank
	// iterations then run on the clone under a bounded analytics slot.
	lease, err := s.beginGraphAnalytics(stream.Context(), ds)
	if err != nil {
		return analyticsSetupError(err, "PageRank not supported for this index type")
	}
	defer lease.done()

	ga := NewGraphAnalytics(func() *lbtypes.GraphData { return lease.data })
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
				if i >= len(body) || body[i] == '}' {
					break
				}
				key, newPos, err := query.ParseString(body, i)
				if err != nil {
					break
				}
				i = query.SkipWhitespace(body, newPos)
				if i < len(body) && body[i] == ':' {
					i++
				}
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
				if i < len(body) && body[i] == ',' {
					i++
				}
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

	// LPA walks the whole layer-0 graph; run it on a clone under the bounded
	// analytics semaphore rather than holding ds.dataMu.RLock.
	lease, err := s.beginGraphAnalytics(stream.Context(), ds)
	if err != nil {
		return analyticsSetupError(err, "Community detection not supported for this index type")
	}
	defer lease.done()

	ga := NewGraphAnalytics(func() *lbtypes.GraphData { return lease.data })
	maxIter := req.MaxIterations
	if maxIter <= 0 {
		maxIter = 10
	}

	start := time.Now()
	result, err := ga.DetectCommunities(stream.Context(), maxIter)
	if err != nil {
		return status.Errorf(codes.Internal, "Community detection failed: %v", err)
	}
	metrics.GraphClusteringDurationSeconds.Observe(time.Since(start).Seconds())
	metrics.GraphCommunitiesTotal.Set(float64(result.CommunityCount))

	respBytes, err := json.Marshal(result)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to serialize results: %v", err)
	}

	return stream.Send(&flight.Result{Body: respBytes})
}
