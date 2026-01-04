package store

import (
	"context"

	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	err := s.VectorStore.DoGet(tkt, stream)
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

// ListFlights returns Unimplemented on DataServer
func (s *DataServer) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	return status.Error(codes.Unimplemented, "ListFlights not implemented on DataServer; use MetaServer")
}

// GetFlightInfo returns Unimplemented on DataServer
func (s *DataServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Error(codes.Unimplemented, "GetFlightInfo not implemented on DataServer; use MetaServer")
}

// GetSchema delegates to VectorStore with error conversion
func (s *DataServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	result, err := s.VectorStore.GetSchema(ctx, desc)
	return result, ToGRPCStatus(err)
}

// DoAction returns Unimplemented on DataServer (data plane only)
func (s *DataServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	return status.Error(codes.Unimplemented, "DoAction not implemented on DataServer; use MetaServer")
}

// MetaServer handles control plane operations (ListFlights, GetFlightInfo)
// Embeds VectorStore to inherit base interface, overrides methods for error conversion.
type MetaServer struct {
	*VectorStore
	coordinator *GlobalSearchCoordinator
}

func NewMetaServer(store *VectorStore) *MetaServer {
	return &MetaServer{
		VectorStore: store,
		coordinator: NewGlobalSearchCoordinator(store.logger),
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

// DoGet returns Unimplemented on MetaServer
func (s *MetaServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	return status.Error(codes.Unimplemented, "DoGet not implemented on MetaServer; use DataServer")
}

// DoPut returns Unimplemented on MetaServer
func (s *MetaServer) DoPut(stream flight.FlightService_DoPutServer) error {
	return status.Error(codes.Unimplemented, "DoPut not implemented on MetaServer; use DataServer")
}

// DoExchange returns Unimplemented on MetaServer
func (s *MetaServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return status.Error(codes.Unimplemented, "DoExchange not implemented")
}

// DoAction handles management commands on MetaServer
func (s *MetaServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action == nil {
		return status.Error(codes.InvalidArgument, "action is required")
	}
	LogClientAction(stream.Context(), s.logger, s.Mesh, "DoAction", map[string]interface{}{
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
	case "GetGraphStats":
		return s.handleGetGraphStats(action.Body, stream)
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
	statusInfo := map[string]interface{}{
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
	// Return success
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
	resp := map[string]interface{}{
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
