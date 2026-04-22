package store

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
)

// DataServer handles data plane operations (DoGet, DoPut)
// Embeds VectorStore to inherit base interface, overrides methods for error conversion.
type DataServer struct {
	*VectorStore
}

func NewDataServer(store *VectorStore) *DataServer {
	return &DataServer{store}
}

// DoGet retrieves a dataset, converting domain errors to gRPC status codes.
func (s *DataServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	LogClientAction(stream.Context(), s.logger, s.Mesh, "DoGet", nil)
	timer := prometheus.NewTimer(metrics.FlightDurationSeconds.WithLabelValues("do_get"))
	defer timer.ObserveDuration()
	metrics.FlightOpsTotal.WithLabelValues("do_get", "start").Inc()

	err := s.VectorStore.DoGet(tkt, stream)
	if err != nil {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "error").Inc()
	} else {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "success").Inc()
	}
	return ToGRPCStatus(err)
}

// DoPut stores a dataset, converting domain errors to gRPC status codes.
func (s *DataServer) DoPut(stream flight.FlightService_DoPutServer) error {
	LogClientAction(stream.Context(), s.logger, s.Mesh, "DoPut", nil)
	// Backpressure Check: If WAL queue is > 80% full, signal client
	depth, queueCap := s.GetWALQueueDepth()
	if queueCap > 0 && float64(depth)/float64(queueCap) > 0.8 {
		// Send metadata as "Warning" - client should slow down
		s.logger.Warn().
			Int("wal_depth", depth).
			Int("wal_cap", queueCap).
			Msg("Applying backpressure")
		metadata := []byte(`{"status": "slow_down", "reason": "wal_pressure"}`)
		if err := stream.Send(&flight.PutResult{AppMetadata: metadata}); err != nil {
			// Log error but proceed, don't fail the whole request just because signaling failed
			s.logger.Error().Err(err).Msg("Failed to send backpressure signal")
		}
	}
	err := s.VectorStore.DoPut(stream)
	return ToGRPCStatus(err)
}

// DoExchange delegates to VectorStore with error conversion
func (s *DataServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	LogClientAction(stream.Context(), s.logger, s.Mesh, "DoExchange", nil)
	err := s.VectorStore.DoExchange(stream)
	return ToGRPCStatus(err)
}

// ListFlights delegates to VectorStore for dataset listing
func (s *DataServer) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	LogClientAction(stream.Context(), s.logger, s.Mesh, "ListFlights", nil)
	err := s.VectorStore.ListFlights(c, stream)
	return ToGRPCStatus(err)
}

// GetFlightInfo returns dataset metadata, delegating to VectorStore
func (s *DataServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	LogClientAction(ctx, s.logger, s.Mesh, "GetFlightInfo", nil)
	info, err := s.VectorStore.GetFlightInfo(ctx, desc)
	return info, ToGRPCStatus(err)
}

// GetSchema delegates to VectorStore with error conversion
func (s *DataServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	result, err := s.VectorStore.GetSchema(ctx, desc)
	return result, ToGRPCStatus(err)
}

// DoAction handles actions on DataServer. Supports VectorSearch for data plane.
func (s *DataServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action != nil && action.Type == "VectorSearch" {
		return s.handleVectorSearchAction(action, stream)
	}
	// Delegate to base VectorStore for other actions (like "delete", "cluster-status")
	return s.VectorStore.DoAction(action, stream)
}

// MetaServer handles control plane operations (ListFlights, GetFlightInfo)
// Embeds VectorStore to inherit base interface.
type MetaServer struct {
	*VectorStore
}

func NewMetaServer(store *VectorStore) *MetaServer {
	coord := NewGlobalSearchCoordinator(store.logger, store.pool)
	store.SetCoordinator(coord)
	return &MetaServer{
		VectorStore: store,
	}
}

// Close cleans up MetaServer resources
func (s *MetaServer) Close() error {
	if s.coordinator != nil {
		return s.coordinator.Close()
	}
	return nil
}

// ListFlights returns available datasets, converting domain errors to gRPC status.
func (s *MetaServer) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	err := s.VectorStore.ListFlights(c, stream)
	return ToGRPCStatus(err)
}

// GetFlightInfo returns dataset metadata, converting domain errors to gRPC status.
func (s *MetaServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	info, err := s.VectorStore.GetFlightInfo(ctx, desc)
	return info, ToGRPCStatus(err)
}

// DoGet retrieves a dataset or executes search, converting domain errors to gRPC status codes.
func (s *MetaServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	LogClientAction(stream.Context(), s.logger, s.Mesh, "DoGet", nil)
	timer := prometheus.NewTimer(metrics.FlightDurationSeconds.WithLabelValues("do_get"))
	defer timer.ObserveDuration()
	metrics.FlightOpsTotal.WithLabelValues("do_get", "start").Inc()

	err := s.VectorStore.DoGet(tkt, stream)
	if err != nil {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "error").Inc()
	} else {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "success").Inc()
	}
	return ToGRPCStatus(err)
}

// DoPut returns Unimplemented on MetaServer
func (s *MetaServer) DoPut(stream flight.FlightService_DoPutServer) error {
	return status.Error(codes.Unimplemented, "DoPut not implemented on MetaServer; use DataServer")
}

// DoExchange delegates to VectorStore
func (s *MetaServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	LogClientAction(stream.Context(), s.logger, s.Mesh, "DoExchange", nil)
	err := s.VectorStore.DoExchange(stream)
	return ToGRPCStatus(err)
}

// DoAction handles management commands on MetaServer
func (s *MetaServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action == nil {
		return status.Error(codes.InvalidArgument, "action is required")
	}
	LogClientAction(stream.Context(), s.logger, s.Mesh, "DoAction", map[string]any{
		"type": action.Type,
	})

	// Route to specific action handlers
	switch action.Type {
	case "VectorSearch":
		return s.handleVectorSearchAction(action, stream)
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
	default:
		return s.VectorStore.DoAction(action, stream)
	}
}

func (s *MetaServer) handleMeshIdentity(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleMeshStatus(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleDiscoveryStatus(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleCreateNamespace(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}
	if err := s.CreateNamespace(req.Name); err != nil {
		return ToGRPCStatus(err)
	}
	// Increment metric for successful namespace creation
	// Note: The original instruction provided a metric for DoPutZeroCopyPathTotal.
	// Assuming the intent was to increment a metric relevant to namespace creation,
	// a placeholder is used here. If a specific metric for namespace creation exists,
	// it should be used instead.
	// metrics.NamespaceCreationTotal.Inc() // Example placeholder
	return stream.Send(&flight.Result{Body: []byte(`{"status": "created"}`)})
}

func (s *MetaServer) handleDeleteNamespace(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}
	if err := s.DeleteNamespace(req.Name); err != nil {
		return ToGRPCStatus(err)
	}
	return stream.Send(&flight.Result{Body: []byte(`{"status": "deleted"}`)})
}

func (s *MetaServer) handleListNamespaces(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleGetTotalNamespaceCount(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleGetNamespaceDatasetCount(action *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleListDatasetsInNamespace(action *flight.Action, stream flight.FlightService_DoActionServer) error {
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

// ToGRPCStatus converts domain errors to gRPC status codes
func ToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	// Already a gRPC status?
	if _, ok := status.FromError(err); ok {
		return err
	}
	// Check internal types
	switch e := err.(type) {
	case *core.ErrNotFound:
		return status.Errorf(codes.NotFound, "%v", e)
	case *core.ErrInvalidArgument:
		return status.Errorf(codes.InvalidArgument, "%v", e)
	case *core.ErrResourceExhausted:
		return status.Errorf(codes.ResourceExhausted, "%v", e)
	case *core.ErrUnavailable:
		return status.Errorf(codes.Unavailable, "%v", e)
	}
	return status.Errorf(codes.Internal, "%v", err)
}

func (s *MetaServer) handleGetCapacityPlan(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleGetAutoScaleConfig(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	config := s.GetAutoScaleConfig()
	data, err := json.Marshal(config)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal auto-scale config: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *MetaServer) handleSetAutoScaleConfig(action *flight.Action, stream flight.FlightService_DoActionServer) error {
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

type CDCSubscribeRequest struct {
	Dataset    string   `json:"dataset"`
	EventTypes []string `json:"event_types,omitempty"`
	Columns    []string `json:"columns,omitempty"`
	BufferSize int      `json:"buffer_size,omitempty"`
}

func (s *MetaServer) handleCDCSubscribe(action *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleCDCUnsubscribe(action *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleCDCGetMetrics(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
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

func (s *MetaServer) handleGetIndexRecommendation(action *flight.Action, stream flight.FlightService_DoActionServer) error {
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
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
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

func (s *MetaServer) handleTemporalSearch(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if s.temporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	var req TemporalSearchRequest
	if len(action.Body) > 0 {
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid temporal search request: %v", err)
		}
	}

	if err := req.Validate(); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	var results []SearchResult
	var err error

	switch req.SearchType {
	case "as_of":
		results, err = s.temporalIndex.SearchAsOf(stream.Context(), req.Timestamp, req.K)
	case "range":
		results, err = s.temporalIndex.SearchRange(stream.Context(), req.StartTime, req.EndTime, req.K)
	case "sliding_window":
		results, err = s.temporalIndex.SearchSlidingWindow(stream.Context(), req.WindowSize, req.K)
	case "sliding_window_time":
		results, err = s.temporalIndex.SearchSlidingWindowByTime(stream.Context(), req.Duration, req.K)
	default:
		results, err = s.temporalIndex.SearchAsOf(stream.Context(), req.Timestamp, req.K)
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

func (s *MetaServer) handleTemporalRangeSearch(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if s.temporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	var req struct {
		StartTime int64 `json:"start_time"`
		EndTime   int64 `json:"end_time"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	vectors := s.temporalIndex.GetVectorsInRange(req.StartTime, req.EndTime)

	data, err := json.Marshal(map[string]interface{}{
		"vectors": vectors,
		"count":   len(vectors),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *MetaServer) handleTemporalVersionHistory(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if s.temporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	var req struct {
		VectorID uint64 `json:"vector_id"`
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	history := s.temporalIndex.GetHistory(req.VectorID)

	data, err := json.Marshal(map[string]interface{}{
		"history": history,
		"count":   len(history),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: data})
}

func (s *MetaServer) handleTemporalAggregation(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if s.temporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	var req struct {
		AggregationType string `json:"aggregation_type"` // count, min, max, mean
		StartTime       int64  `json:"start_time"`
		EndTime         int64  `json:"end_time"`
		Interval        int64  `json:"interval"` // bucket interval in nanoseconds
	}
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	if req.Interval <= 0 {
		req.Interval = 3600000000000 // 1 hour default
	}

	vectors := s.temporalIndex.GetVectorsInRange(req.StartTime, req.EndTime)
	s.logger.Info().Int64("start", req.StartTime).Int64("end", req.EndTime).Int("count", len(vectors)).Msg("Temporal aggregation vectors found")

	type bucket struct {
		Timestamp int64 `json:"timestamp"`
		Count     int   `json:"count"`
	}

	var buckets []bucket
	if req.AggregationType == "count" {
		bucketMap := make(map[int64]int)
		for _, v := range vectors {
			bucketTs := (v.Timestamp.UnixNano() / req.Interval) * req.Interval
			bucketMap[bucketTs]++
		}
		for ts, count := range bucketMap {
			buckets = append(buckets, bucket{Timestamp: ts, Count: count})
		}
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].Timestamp < buckets[j].Timestamp
		})
	}

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
