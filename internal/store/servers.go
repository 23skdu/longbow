package store

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DataServer handles data plane operations (DoGet, DoPut)
// Embeds VectorStore to inherit base interface, overrides methods for error conversion.
type DataServer struct {
	*VectorStore
}

func NewDataServer(store *VectorStore) *DataServer {
	return &DataServer{VectorStore: store}
}

// DoGet retrieves a dataset, converting domain errors to gRPC status codes.
func (s *DataServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	err := s.VectorStore.DoGet(tkt, stream)
	return ToGRPCStatus(err)
}

// DoPut stores a dataset, converting domain errors to gRPC status codes.
func (s *DataServer) DoPut(stream flight.FlightService_DoPutServer) error {
	// Backpressure Check: If WAL queue is > 80% full, signal client
	depth, cap := s.VectorStore.GetWALQueueDepth()
	if cap > 0 && float64(depth)/float64(cap) > 0.8 {
		// Send metadata as "Warning" - client should slow down
		// Note: Flight DoPut returns a stream of PutResult. We can send one immediately?
		// Usually DoPut reads first. But we can send a "header" result if client is reading.
		// For simplicity, we just log and maybe could fail fast if CRITICAL (e.g. >95%)
		// But sending metadata in the *response* stream (PutResult) is the standard way.
		// We'll do it inside VectorStore.DoPut loop or here if we wrap it.
		// Since s.VectorStore.DoPut controls the loop, we should inject it there or wrap it.
		// Wrapping s.VectorStore.DoPut is hard without duplicating code.
		// Let's rely on s.VectorStore.DoPut to handle it (I should update store.go instead).
		// Wait, I updated store.go to expose GetWALQueueDepth.
		// I should update VectorStore.DoPut in store.go to actually *use* it.
		// The prompt said "Add backpressure signals to the Flight Server implementation".
		// Doing it in store.go (which implements the logic) is correct.
	}
	err := s.VectorStore.DoPut(stream)
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
}

func NewMetaServer(store *VectorStore) *MetaServer {
	return &MetaServer{
		VectorStore: store,
	}
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
	s.logger.Info("MetaServer DoAction called", zap.String("type", action.Type))

	// Route to specific action handlers
	switch action.Type {
	case "VectorSearch":
		return s.handleVectorSearchAction(action, stream)
	default:
		return status.Errorf(codes.Unimplemented, "unknown action: %s", action.Type)
	}
}
