package store

import (
"context"

"github.com/apache/arrow/go/v18/arrow/flight"
"google.golang.org/grpc/codes"
"google.golang.org/grpc/status"
)

// DataServer handles data plane operations (DoGet, DoPut)
type DataServer struct {
*VectorStore
}

func NewDataServer(store *VectorStore) *DataServer {
return &DataServer{VectorStore: store}
}

// ListFlights returns Unimplemented on DataServer
func (s *DataServer) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
return status.Error(codes.Unimplemented, "ListFlights is not implemented on DataServer; use MetaServer")
}

// GetFlightInfo returns Unimplemented on DataServer
func (s *DataServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
return nil, status.Error(codes.Unimplemented, "GetFlightInfo is not implemented on DataServer; use MetaServer")
}

// MetaServer handles control plane operations (ListFlights, GetFlightInfo)
type MetaServer struct {
*VectorStore
}

func NewMetaServer(store *VectorStore) *MetaServer {
return &MetaServer{VectorStore: store}
}

// DoGet returns Unimplemented on MetaServer
func (s *MetaServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
return status.Error(codes.Unimplemented, "DoGet is not implemented on MetaServer; use DataServer")
}

// DoPut returns Unimplemented on MetaServer
func (s *MetaServer) DoPut(stream flight.FlightService_DoPutServer) error {
return status.Error(codes.Unimplemented, "DoPut is not implemented on MetaServer; use DataServer")
}

// DoAction returns Unimplemented on MetaServer (assuming actions are data-related or we want to restrict them)
// For now, let's keep DoAction on DataServer as it modifies state (drop_dataset) or gets stats.
// Actually, get_stats might be useful on MetaServer too, but let's stick to the plan of separating traffic.
// If DoAction is called on MetaServer, we should probably return Unimplemented unless we decide otherwise.
// The prompt didn't explicitly mention DoAction, but usually management is on Meta or Data depending on architecture.
// Given "drop_dataset" modifies data, it fits DataServer. "get_stats" fits both but let's default to DataServer for now to keep Meta pure metadata lookup.
// Wait, the prompt says "segregate read and write traffic" but also "contention on heavy data streams blocking metadata lookups".
// So MetaServer should be lightweight.
// Let's explicitly disable DoAction on MetaServer for now to be safe.

func (s *MetaServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
return status.Error(codes.Unimplemented, "DoAction is not implemented on MetaServer; use DataServer")
}
