package store

import (
	"encoding/json"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

func (s *VectorStore) handleMeshIdentity(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	if s.Mesh == nil {
		return status.Error(codes.FailedPrecondition, "mesh is not initialized")
	}
	identity := s.Mesh.GetIdentity()
	body, err := json.Marshal(identity)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal identity: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}

func (s *VectorStore) handleMeshStatus(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	if s.Mesh == nil {
		return status.Error(codes.FailedPrecondition, "mesh is not initialized")
	}

	// Get member count for cache validation
	members := s.Mesh.GetMembers()
	memberCount := len(members)

	// Try cache first
	if s.meshStatusCache != nil {
		if cached := s.meshStatusCache.Get(memberCount); cached != nil {
			return stream.Send(&flight.Result{Body: cached})
		}
	}

	// Cache miss - serialize with pooled encoder
	buf, enc := GetJSONEncoder()
	defer PutJSONEncoder(buf, enc)

	if err := enc.Encode(members); err != nil {
		return status.Errorf(codes.Internal, "failed to marshal members: %v", err)
	}

	body := buf.Bytes()

	// Update cache
	if s.meshStatusCache != nil {
		s.meshStatusCache.Set(body, memberCount)
	}

	return stream.Send(&flight.Result{Body: body})
}

func (s *VectorStore) handleDiscoveryStatus(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	if s.Mesh == nil {
		return status.Error(codes.FailedPrecondition, "mesh is not initialized")
	}
	provider, peers := s.Mesh.GetDiscoveryStatus()
	statusInfo := map[string]any{
		"provider": provider,
		"peers":    peers,
	}
	body, err := json.Marshal(statusInfo)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal discovery status: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}

func (s *VectorStore) handleCreateNamespace(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}
	if err := s.CreateNamespace(req.Name); err != nil {
		return types.ToGRPCStatus(err)
	}
	metrics.NamespaceCreationTotal.Inc()
	return stream.Send(&flight.Result{Body: []byte(`{"status": "created"}`)})
}

func (s *VectorStore) handleDeleteNamespace(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}
	if err := s.DeleteNamespace(req.Name); err != nil {
		return types.ToGRPCStatus(err)
	}
	return stream.Send(&flight.Result{Body: []byte(`{"status": "deleted"}`)})
}

func (s *VectorStore) handleListNamespaces(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	names := s.ListNamespaces()
	resp := map[string]any{
		"namespaces": names,
		"count":      len(names),
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}

func (s *VectorStore) handleGetTotalNamespaceCount(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	count := s.GetTotalNamespaceCount()
	resp := map[string]int{
		"count": count,
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}

func (s *VectorStore) handleGetNamespaceDatasetCount(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	if !s.NamespaceExists(req.Name) {
		return status.Errorf(codes.NotFound, "namespace not found: %s", req.Name)
	}

	count := s.GetNamespaceDatasetCount(req.Name)
	resp := map[string]int{
		"count": count,
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}

func (s *VectorStore) handleListDatasetsInNamespace(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	if !s.NamespaceExists(req.Name) {
		return status.Errorf(codes.NotFound, "namespace not found: %s", req.Name)
	}

	datasets := s.ListDatasetsInNamespace(req.Name)
	resp := map[string][]string{
		"datasets": datasets,
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}

// types.ToGRPCStatus converts domain errors to gRPC status codes

func (s *VectorStore) handleGetCapacityPlan(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	plan, err := s.GetCapacityPlan()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get capacity plan: %v", err)
	}
	data, err := json.Marshal(plan)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal capacity plan: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleGetAutoScaleConfig(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	config := s.GetAutoScaleConfig()
	data, err := json.Marshal(config)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal auto-scale config: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleSetAutoScaleConfig(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req AutoScaleConfig
	if len(action.Body) > 0 {
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid auto-scale config: %v", err)
		}
	}
	if err := s.SetAutoScaleConfig(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to set auto-scale config: %v", err)
	}
	data, err := json.Marshal(req)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

// CDCSubscribeRequest defines the request body for CDC subscription actions.
type CDCSubscribeRequest struct {
	Dataset    string   `json:"dataset"`
	EventTypes []string `json:"event_types,omitempty"`
	Columns    []string `json:"columns,omitempty"`
	BufferSize int      `json:"buffer_size,omitempty"`
}

func (s *VectorStore) handleCDCSubscribe(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req CDCSubscribeRequest
	if len(action.Body) > 0 {
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid CDC subscribe request: %v", err)
		}
	}

	if req.BufferSize <= 0 {
		req.BufferSize = 1024
	}

	filter := CDCFilter{
		EventTypes: []CDCEventType{},
		Columns:    req.Columns,
	}

	sub, err := s.cdc.Subscribe(req.Dataset, filter, req.BufferSize)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create CDC subscription: %v", err)
	}

	resp := map[string]interface{}{
		"subscription_id": sub.ID,
		"dataset":         req.Dataset,
		"status":          "subscribed",
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleCDCUnsubscribe(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if len(action.Body) > 0 {
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid CDC unsubscribe request: %v", err)
		}
	}

	if err := s.cdc.Unsubscribe(req.SubscriptionID); err != nil {
		return status.Errorf(codes.Internal, "failed to unsubscribe: %v", err)
	}

	resp := map[string]interface{}{
		"subscription_id": req.SubscriptionID,
		"status":          "unsubscribed",
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleCDCGetMetrics(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	received, sent, dropped, filtered, subs, full := s.cdc.GetMetrics()

	resp := map[string]interface{}{
		"events_received": received,
		"events_sent":     sent,
		"events_dropped":  dropped,
		"events_filtered": filtered,
		"subscriptions":   subs,
		"channel_full":    full,
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal metrics: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleGetIndexRecommendation(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		VectorDimension int     `json:"vector_dimension"`
		NumQueryVectors int     `json:"num_query_vectors"`
		SearchK         int     `json:"search_k"`
		DatasetSize     int     `json:"dataset_size"`
		NumCollections  int     `json:"num_collections"`
		QueryComplexity string  `json:"query_complexity"`
		AvgVectorNorm   float64 `json:"avg_vector_norm"`
		IsFiltered      bool    `json:"is_filtered"`
		IsHybrid        bool    `json:"is_hybrid"`
	}
	if len(action.Body) > 0 {
		i := 0
		i = query.SkipWhitespace(action.Body, i)
		if i < len(action.Body) && action.Body[i] == '{' {
			i++
			for i < len(action.Body) {
				i = query.SkipWhitespace(action.Body, i)
				if i >= len(action.Body) || action.Body[i] == '}' {
					break
				}
				key, newPos, err := query.ParseString(action.Body, i)
				if err != nil {
					break
				}
				i = query.SkipWhitespace(action.Body, newPos)
				if i < len(action.Body) && action.Body[i] == ':' {
					i++
				}
				i = query.SkipWhitespace(action.Body, i)
				switch key {
				case "vector_dimension":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.VectorDimension = int(val)
					i = newPos
				case "num_query_vectors":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.NumQueryVectors = int(val)
					i = newPos
				case "search_k":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.SearchK = int(val)
					i = newPos
				case "dataset_size":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.DatasetSize = int(val)
					i = newPos
				case "num_collections":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.NumCollections = int(val)
					i = newPos
				case "query_complexity":
					val, newPos, _ := query.ParseString(action.Body, i)
					req.QueryComplexity = val
					i = newPos
				case "avg_vector_norm":
					val, newPos, _ := query.ParseFloat32(action.Body, i)
					req.AvgVectorNorm = float64(val)
					i = newPos
				case "is_filtered":
					val, newPos, _ := query.ParseBool(action.Body, i)
					req.IsFiltered = val
					i = newPos
				case "is_hybrid":
					val, newPos, _ := query.ParseBool(action.Body, i)
					req.IsHybrid = val
					i = newPos
				default:
					i, _ = query.SkipValue(action.Body, i)
				}
				i = query.SkipWhitespace(action.Body, i)
				if i < len(action.Body) && action.Body[i] == ',' {
					i++
				}
			}
		}
	}

	features := QueryFeatures{
		VectorDimension: req.VectorDimension,
		NumQueryVectors: req.NumQueryVectors,
		SearchK:         req.SearchK,
		DatasetSize:     req.DatasetSize,
		NumCollections:  req.NumCollections,
		QueryComplexity: req.QueryComplexity,
		AvgVectorNorm:   req.AvgVectorNorm,
		IsFiltered:      req.IsFiltered,
		IsHybrid:        req.IsHybrid,
	}

	prediction := s.GetIndexRecommendation(features)
	data, err := json.Marshal(prediction)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal prediction: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleTemporalSearch(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if !s.temporalConfig.Enabled {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	var req TemporalSearchRequest
	if len(action.Body) > 0 {
		parser := s.temporalParserPool.Get().(*query.ZeroAllocTemporalParser)
		defer s.temporalParserPool.Put(parser)

		var err error
		req, err = parser.ParseSearch(action.Body)
		if err != nil {
			// Fallback to standard unmarshal if zero-alloc parser fails
			req = TemporalSearchRequest{}
			if err := json.Unmarshal(action.Body, &req); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid temporal search request: %v", err)
			}
		}
	}

	if err := req.Validate(); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}
	if ds.TemporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not initialized for dataset")
	}

	var results []SearchResult
	var err error

	switch req.SearchType {
	case "as_of":
		results, err = ds.TemporalIndex.SearchAsOf(stream.Context(), req.Timestamp, req.K)
	case "range":
		results, err = ds.TemporalIndex.SearchRange(stream.Context(), req.StartTime, req.EndTime, req.K)
	case "sliding_window":
		results, err = ds.TemporalIndex.SearchSlidingWindow(stream.Context(), req.WindowSize, req.K)
	case "sliding_window_time":
		results, err = ds.TemporalIndex.SearchSlidingWindowByTime(stream.Context(), req.Duration, req.K)
	default:
		results, err = ds.TemporalIndex.SearchAsOf(stream.Context(), req.Timestamp, req.K)
	}

	if err != nil {
		return status.Errorf(codes.Internal, "temporal search failed: %v", err)
	}

	type temporalResult struct {
		ID       uint64  `json:"id"`
		Distance float32 `json:"distance"`
		Score    float32 `json:"score"`
	}

	respResults := make([]temporalResult, len(results))
	for i, r := range results {
		respResults[i] = temporalResult{
			ID:       uint64(r.ID),
			Distance: r.Distance,
			Score:    r.Score,
		}
	}

	data, err := json.Marshal(map[string]interface{}{
		"results": respResults,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleTemporalRangeSearch(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if !s.temporalConfig.Enabled {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	var req struct {
		Dataset   string `json:"dataset"`
		StartTime int64  `json:"start_time"`
		EndTime   int64  `json:"end_time"`
	}
	if len(action.Body) > 0 {
		i := 0
		i = query.SkipWhitespace(action.Body, i)
		if i < len(action.Body) && action.Body[i] == '{' {
			i++
			for i < len(action.Body) {
				i = query.SkipWhitespace(action.Body, i)
				if i >= len(action.Body) || action.Body[i] == '}' {
					break
				}
				key, newPos, err := query.ParseString(action.Body, i)
				if err != nil {
					break
				}
				i = query.SkipWhitespace(action.Body, newPos)
				if i < len(action.Body) && action.Body[i] == ':' {
					i++
				}
				i = query.SkipWhitespace(action.Body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(action.Body, i)
					req.Dataset = val
					i = newPos
				case "start_time":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.StartTime = val
					i = newPos
				case "end_time":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.EndTime = val
					i = newPos
				default:
					i, _ = query.SkipValue(action.Body, i)
				}
				i = query.SkipWhitespace(action.Body, i)
				if i < len(action.Body) && action.Body[i] == ',' {
					i++
				}
			}
		}
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}
	if ds.TemporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not initialized for dataset")
	}

	vectors := ds.TemporalIndex.GetVectorsInRange(req.StartTime, req.EndTime)

	data, err := json.Marshal(map[string]interface{}{
		"vectors": vectors,
		"count":   len(vectors),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleTemporalVersionHistory(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if !s.temporalConfig.Enabled {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	var req core.TemporalVersionHistoryRequest
	if len(action.Body) > 0 {
		i := 0
		i = query.SkipWhitespace(action.Body, i)
		if i < len(action.Body) && action.Body[i] == '{' {
			i++
			for i < len(action.Body) {
				i = query.SkipWhitespace(action.Body, i)
				if i >= len(action.Body) || action.Body[i] == '}' {
					break
				}
				key, newPos, err := query.ParseString(action.Body, i)
				if err != nil {
					break
				}
				i = query.SkipWhitespace(action.Body, newPos)
				if i < len(action.Body) && action.Body[i] == ':' {
					i++
				}
				i = query.SkipWhitespace(action.Body, i)
				switch key {
				case "dataset":
					val, newPos, _ := query.ParseString(action.Body, i)
					req.Dataset = val
					i = newPos
				case "vector_id":
					val, newPos, _ := query.ParseInt64(action.Body, i)
					req.VectorID = uint64(val) // #nosec G115 -- val is within uint64 range
					i = newPos
				default:
					i, _ = query.SkipValue(action.Body, i)
				}
				i = query.SkipWhitespace(action.Body, i)
				if i < len(action.Body) && action.Body[i] == ',' {
					i++
				}
			}
		}
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}
	if ds.TemporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not initialized for dataset")
	}

	history := ds.TemporalIndex.GetHistory(req.VectorID)

	data, err := json.Marshal(map[string]interface{}{
		"history": history,
		"count":   len(history),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *VectorStore) handleTemporalAggregation(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if !s.temporalConfig.Enabled {
		return status.Error(codes.FailedPrecondition, "temporal index or aggregator not enabled")
	}

	var req TemporalAggregationRequest
	if len(action.Body) > 0 {
		parser := s.temporalParserPool.Get().(*query.ZeroAllocTemporalParser)
		defer s.temporalParserPool.Put(parser)

		var err error
		req, err = parser.ParseAggregation(action.Body)
		if err != nil {
			// Fallback to standard unmarshal
			req = TemporalAggregationRequest{}
			if err := json.Unmarshal(action.Body, &req); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
			}
		}
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}
	if ds.TemporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not initialized for dataset")
	}

	aggReq := TemporalAggRequest{
		AggType:     TemporalAggType(req.AggregationType),
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		Interval:    req.Interval,
		MetricField: req.MetricField,
	}

	vectors := ds.TemporalIndex.GetVectorsInRange(req.StartTime, req.EndTime)
	s.logger.Info().
		Int64("start", req.StartTime).
		Int64("end", req.EndTime).
		Int("count", len(vectors)).
		Str("type", req.AggregationType).
		Msg("Executing temporal aggregation")

	aggregator := NewTemporalAggregator(int(s.temporalConfig.MaxBuckets))
	buckets := aggregator.Aggregate(aggReq, vectors)

	data, err := json.Marshal(map[string]interface{}{
		"aggregation_type": req.AggregationType,
		"buckets":          buckets,
		"total_count":      len(vectors),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}
func (s *VectorStore) handleGraphRAGExpand(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Dataset string   `json:"dataset"`
		NodeIDs []uint32 `json:"node_ids"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	neighbors, err := s.GetNeighborsBulk(stream.Context(), req.Dataset, req.NodeIDs)
	if err != nil {
		return types.ToGRPCStatus(err)
	}

	body, err := json.Marshal(map[string]any{
		"neighbors": neighbors,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}

func (s *VectorStore) handleResetDataset(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	if err := s.DropDataset(stream.Context(), req.Name); err != nil {
		return types.ToGRPCStatus(err)
	}

	return stream.Send(&flight.Result{Body: []byte(`{"status": "reset_success"}`)})
}

// doMetaAction handles management commands on VectorStore
// DoMetaAction handles meta-specific actions for the cluster MetaServer.
func (s *VectorStore) DoMetaAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action == nil {
		return status.Error(codes.InvalidArgument, "action is required")
	}

	// Route to specific action handlers
	switch action.Type {
	case "VectorSearch":
		return s.HandleVectorSearchAction(action, stream)
	case "VectorSearchByID":
		return s.handleVectorSearchByIDAction(action, stream)
	case "MeshIdentity":
		return s.handleMeshIdentity(action, stream)
	case "MeshStatus":
		return s.handleMeshStatus(action, stream)
	case "DiscoveryStatus":
		return s.handleDiscoveryStatus(action, stream)
	case "CreateNamespace":
		return s.handleCreateNamespace(action, stream)
	case "DeleteNamespace":
		return s.handleDeleteNamespace(action, stream)
	case "ListNamespaces":
		return s.handleListNamespaces(action, stream)
	case "GetTotalNamespaceCount":
		return s.handleGetTotalNamespaceCount(action, stream)
	case "GetNamespaceDatasetCount":
		return s.handleGetNamespaceDatasetCount(action, stream)
	case "ListDatasetsInNamespace":
		return s.handleListDatasetsInNamespace(action, stream)
	case "GetGraphStats":
		return s.handleGetGraphStats(action.Body, stream)
	case "GetCapacityPlan":
		return s.handleGetCapacityPlan(action, stream)
	case "GetAutoScaleConfig":
		return s.handleGetAutoScaleConfig(action, stream)
	case "SetAutoScaleConfig":
		return s.handleSetAutoScaleConfig(action, stream)
	case "CDCSubscribe":
		return s.handleCDCSubscribe(action, stream)
	case "CDCUnsubscribe":
		return s.handleCDCUnsubscribe(action, stream)
	case "CDCGetMetrics":
		return s.handleCDCGetMetrics(action, stream)
	case "GetIndexRecommendation":
		return s.handleGetIndexRecommendation(action, stream)
	case "TemporalSearch":
		return s.handleTemporalSearch(action, stream)
	case "TemporalRangeSearch":
		return s.handleTemporalRangeSearch(action, stream)
	case "TemporalVersionHistory":
		return s.handleTemporalVersionHistory(action, stream)
	case "TemporalAggregation":
		return s.handleTemporalAggregation(action, stream)
	case "GraphRAGExpand":
		return s.handleGraphRAGExpand(action, stream)
	case "ResetDataset":
		return s.handleResetDataset(action, stream)
	default:
		return status.Errorf(codes.Unimplemented, "unimplemented action: %s", action.Type)
	}
}
