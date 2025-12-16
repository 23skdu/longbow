package store

import (
"bytes"
"context"
"encoding/json"

"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/ipc"
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

// MetaServer handles control plane operations (ListFlights, GetFlightInfo) and Analytics
// Embeds VectorStore to inherit base interface, overrides methods for error conversion.
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

// DoAction handles management and analytics commands on MetaServer
func (s *MetaServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
if action == nil {
return status.Error(codes.InvalidArgument, "action is required")
}
s.logger.Info("MetaServer DoAction called", "type", action.Type)

switch action.Type {
case "query_analytics":
return s.handleQueryAnalytics(action, stream)
default:
return status.Errorf(codes.Unimplemented, "unknown action: %s", action.Type)
}
}

// handleQueryAnalytics processes analytics queries via DuckDB
func (s *MetaServer) handleQueryAnalytics(action *flight.Action, stream flight.FlightService_DoActionServer) error {
var req struct {
Dataset string `json:"dataset"`
Query   string `json:"query"`
}
if err := json.Unmarshal(action.Body, &req); err != nil {
return status.Errorf(codes.InvalidArgument, "invalid JSON body: %v", err)
}

if req.Dataset == "" || req.Query == "" {
return status.Error(codes.InvalidArgument, "dataset and query are required")
}

// Execute via DuckDB Adapter
rdr, cleanup, err := s.duckDB.QuerySnapshot(stream.Context(), req.Dataset, req.Query)
if err != nil {
s.logger.Error("Analytics query failed", "dataset", req.Dataset, "error", err)
return status.Errorf(codes.Internal, "query failed: %v", err)
}
defer cleanup()

// Serialize Arrow Records to IPC stream
var buf bytes.Buffer
writer := ipc.NewWriter(&buf, ipc.WithSchema(rdr.Schema()))

for rdr.Next() {
rec := rdr.Record()
if err := writer.Write(rec); err != nil {
return status.Errorf(codes.Internal, "failed to write Arrow record: %v", err)
}
}
if err := rdr.Err(); err != nil {
return status.Errorf(codes.Internal, "error reading Arrow results: %v", err)
}

if err := writer.Close(); err != nil {
return status.Errorf(codes.Internal, "failed to close IPC writer: %v", err)
}

// Send result back
return stream.Send(&flight.Result{Body: buf.Bytes()})
}
