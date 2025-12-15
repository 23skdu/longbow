package store

import (
"context"
"encoding/json"

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

// MetaServer handles control plane operations (ListFlights, GetFlightInfo) and Analytics
type MetaServer struct {
*VectorStore
duckDB *DuckDBAdapter
}

func NewMetaServer(store *VectorStore) *MetaServer {
return &MetaServer{
VectorStore: store,
duckDB:      NewDuckDBAdapter(store.dataPath),
}
}

// DoGet returns Unimplemented on MetaServer
func (s *MetaServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
return status.Error(codes.Unimplemented, "DoGet is not implemented on MetaServer; use DataServer")
}

// DoPut returns Unimplemented on MetaServer
func (s *MetaServer) DoPut(stream flight.FlightService_DoPutServer) error {
return status.Error(codes.Unimplemented, "DoPut is not implemented on MetaServer; use DataServer")
}

// DoAction handles management and analytics commands on MetaServer
func (s *MetaServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
s.logger.Info("MetaServer DoAction called", "type", action.Type)

switch action.Type {
case "query_analytics":
// Expects JSON body: { "dataset": "name", "query": "SELECT ..." }
var req struct {
Dataset string `json:"dataset"`
Query   string `json:"query"`
}
if err := json.Unmarshal(action.Body, &req); err != nil {
return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
}

if req.Dataset == "" || req.Query == "" {
return status.Error(codes.InvalidArgument, "dataset and query are required")
}

// Execute via DuckDB Adapter
jsonResult, err := s.duckDB.QuerySnapshot(req.Dataset, req.Query)
if err != nil {
s.logger.Error("Analytics query failed", "error", err)
return status.Errorf(codes.Internal, "query failed: %v", err)
}

// Send result back
if err := stream.Send(&flight.Result{Body: []byte(jsonResult)}); err != nil {
return err
}
return nil

default:
return status.Errorf(codes.Unimplemented, "unknown action: %s", action.Type)
}
}
