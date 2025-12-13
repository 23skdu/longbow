package store

import (
"context"
"fmt"
"log/slog"
"sync"

"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/ipc"
"github.com/apache/arrow/go/v18/arrow/memory"
)

// VectorStore implements flight.FlightServer
type VectorStore struct {
flight.BaseFlightServer
mem      memory.Allocator
logger   *slog.Logger
mu       sync.RWMutex
vectors  map[string]arrow.Record
}

func NewVectorStore(mem memory.Allocator, logger *slog.Logger) *VectorStore {
return &VectorStore{
mem:     mem,
logger:  logger,
vectors: make(map[string]arrow.Record),
}
}

// ListFlights returns available streams
func (s *VectorStore) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
s.logger.Info("ListFlights called")
s.mu.RLock()
defer s.mu.RUnlock()

for name := range s.vectors {
info := &flight.FlightInfo{
FlightDescriptor: &flight.FlightDescriptor{
Type: flight.DescriptorPATH,
Path: []string{name},
},
}
if err := stream.Send(info); err != nil {
s.logger.Error("Failed to send flight info", "error", err, "name", name)
return err
}
}
return nil
}

// GetFlightInfo returns metadata for a specific stream
func (s *VectorStore) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
if len(desc.Path) == 0 {
s.logger.Warn("GetFlightInfo called with invalid path")
return nil, fmt.Errorf("invalid path")
}
name := desc.Path[0]

s.mu.RLock()
rec, ok := s.vectors[name]
s.mu.RUnlock()

if !ok {
s.logger.Warn("Vector not found", "name", name)
return nil, fmt.Errorf("vector not found: %s", name)
}

return &flight.FlightInfo{
Schema: flight.SerializeSchema(rec.Schema(), s.mem),
FlightDescriptor: desc,
TotalRecords: rec.NumRows(),
TotalBytes: -1,
}, nil
}

// DoGet streams data to the client
func (s *VectorStore) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
name := string(tkt.Ticket)
s.logger.Info("DoGet called", "ticket", name)

s.mu.RLock()
rec, ok := s.vectors[name]
s.mu.RUnlock()

if !ok {
s.logger.Warn("Vector not found for ticket", "ticket", name)
return fmt.Errorf("vector not found: %s", name)
}

// Use flight.NewRecordWriter directly with the stream
w := flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
defer w.Close()

w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{name}})
if err := w.Write(rec); err != nil {
s.logger.Error("Failed to write record", "error", err, "ticket", name)
return err
}
return nil
}

// DoPut accepts data from the client
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
r, err := flight.NewRecordReader(stream)
if err != nil {
s.logger.Error("Failed to create record reader", "error", err)
return err
}
defer r.Release()

for r.Next() {
rec := r.Record()
rec.Retain()

// Simplified: Store everything under "default" for now as Reader descriptor access is tricky in v18
// In a real app, we'd parse the descriptor from the stream metadata or first message
name := "default"
s.logger.Info("Storing vector", "name", name, "rows", rec.NumRows())

s.mu.Lock()
if old, exists := s.vectors[name]; exists {
old.Release()
}
s.vectors[name] = rec
s.mu.Unlock()
}
return nil
}
