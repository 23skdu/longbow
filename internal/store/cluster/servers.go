package cluster

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"

)

// DataServer handles data plane operations (DoGet, DoPut)
// Embeds VectorStore to inherit base interface, overrides methods for error conversion.
type DataServer struct {
	FlightBackend
}

// NewDataServer creates a new DataServer wrapping the provided VectorStore.
func NewDataServer(store FlightBackend) *DataServer {
	return &DataServer{store}
}

// DoGet retrieves a dataset, converting domain errors to gRPC status codes.
func (s *DataServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	types.LogClientAction(stream.Context(), s.GetLogger(), s.GetMesh(), "DoGet", nil)
	timer := prometheus.NewTimer(metrics.FlightDurationSeconds.WithLabelValues("do_get"))
	defer timer.ObserveDuration()
	metrics.FlightOpsTotal.WithLabelValues("do_get", "start").Inc()

	err := s.FlightBackend.DoGet(tkt, stream)
	if err != nil {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "error").Inc()
	} else {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "success").Inc()
	}
	return types.ToGRPCStatus(err)
}

// DoPut stores a dataset, converting domain errors to gRPC status codes.
func (s *DataServer) DoPut(stream flight.FlightService_DoPutServer) error {
	types.LogClientAction(stream.Context(), s.GetLogger(), s.GetMesh(), "DoPut", nil)
	// Backpressure Check: If WAL queue is > 80% full, signal client
	depth, queueCap := s.GetWALQueueDepth()
	if queueCap > 0 && float64(depth)/float64(queueCap) > 0.8 {
		// Send metadata as "Warning" - client should slow down
		logger := s.GetLogger()
		logger.Warn().
			Int("wal_depth", depth).
			Int("wal_cap", queueCap).
			Msg("Applying backpressure")
		metadata := []byte(`{"status": "slow_down", "reason": "wal_pressure"}`)
		if err := stream.Send(&flight.PutResult{AppMetadata: metadata}); err != nil {
			// Log error but proceed, don't fail the whole request just because signaling failed
			logger.Error().Err(err).Msg("Failed to send backpressure signal")
		}
	}
	err := s.FlightBackend.DoPut(stream)
	return types.ToGRPCStatus(err)
}

// DoExchange delegates to VectorStore with error conversion
func (s *DataServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	types.LogClientAction(stream.Context(), s.GetLogger(), s.GetMesh(), "DoExchange", nil)
	err := s.FlightBackend.DoExchange(stream)
	return types.ToGRPCStatus(err)
}

// ListFlights delegates to VectorStore for dataset listing
func (s *DataServer) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	types.LogClientAction(stream.Context(), s.GetLogger(), s.GetMesh(), "ListFlights", nil)
	err := s.FlightBackend.ListFlights(c, stream)
	return types.ToGRPCStatus(err)
}

// GetFlightInfo returns dataset metadata, delegating to VectorStore
func (s *DataServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	types.LogClientAction(ctx, s.GetLogger(), s.GetMesh(), "GetFlightInfo", nil)
	info, err := s.FlightBackend.GetFlightInfo(ctx, desc)
	return info, types.ToGRPCStatus(err)
}

// GetSchema delegates to VectorStore with error conversion
func (s *DataServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	result, err := s.FlightBackend.GetSchema(ctx, desc)
	return result, types.ToGRPCStatus(err)
}

// DoAction handles actions on DataServer. Supports VectorSearch for data plane.
func (s *DataServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action != nil && action.Type == "VectorSearch" {
		return s.FlightBackend.HandleVectorSearchAction(action, stream)
	}
	// Delegate to base VectorStore for other actions (like "delete", "cluster-status")
	return s.FlightBackend.DoAction(action, stream)
}

// MetaServer handles control plane operations (ListFlights, GetFlightInfo)
// Embeds VectorStore to inherit base interface.
type MetaServer struct {
	FlightBackend
}

// NewMetaServer creates a new MetaServer wrapping the provided FlightBackend.
func NewMetaServer(store FlightBackend) *MetaServer {
	return &MetaServer{
		FlightBackend: store,
	}
}

// Close cleans up MetaServer resources
func (s *MetaServer) Close() error {
	return nil
}

// ListFlights returns available datasets, converting domain errors to gRPC status.
func (s *MetaServer) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	err := s.FlightBackend.ListFlights(c, stream)
	return types.ToGRPCStatus(err)
}

// GetFlightInfo returns dataset metadata, converting domain errors to gRPC status.
func (s *MetaServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	info, err := s.FlightBackend.GetFlightInfo(ctx, desc)
	return info, types.ToGRPCStatus(err)
}

// DoGet retrieves a dataset or executes search, converting domain errors to gRPC status codes.
func (s *MetaServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	types.LogClientAction(stream.Context(), s.GetLogger(), s.GetMesh(), "DoGet", nil)
	timer := prometheus.NewTimer(metrics.FlightDurationSeconds.WithLabelValues("do_get"))
	defer timer.ObserveDuration()
	metrics.FlightOpsTotal.WithLabelValues("do_get", "start").Inc()

	err := s.FlightBackend.DoGet(tkt, stream)
	if err != nil {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "error").Inc()
	} else {
		metrics.FlightOpsTotal.WithLabelValues("do_get", "success").Inc()
	}
	return types.ToGRPCStatus(err)
}

// DoPut returns Unimplemented on MetaServer
func (s *MetaServer) DoPut(stream flight.FlightService_DoPutServer) error {
	return status.Error(codes.Unimplemented, "DoPut not implemented on MetaServer; use DataServer")
}

// DoExchange delegates to VectorStore
func (s *MetaServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	types.LogClientAction(stream.Context(), s.GetLogger(), s.GetMesh(), "DoExchange", nil)
	err := s.FlightBackend.DoExchange(stream)
	return types.ToGRPCStatus(err)
}

// DoAction handles management commands on MetaServer
func (s *MetaServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action == nil {
		return status.Error(codes.InvalidArgument, "action is required")
	}
	types.LogClientAction(stream.Context(), s.GetLogger(), s.GetMesh(), "DoAction", map[string]any{
		"type": action.Type,
	})

	return s.FlightBackend.DoMetaAction(action, stream)
}
