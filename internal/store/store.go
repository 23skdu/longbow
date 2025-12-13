package store

import (
"fmt"
"sync"

"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/array"
"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/memory"
)

// VectorStore implements flight.FlightServer
type VectorStore struct {
flight.BaseFlightServer
mem      memory.Allocator
mu       sync.RWMutex
vectors  map[string]arrow.Record
}

func NewVectorStore(mem memory.Allocator) *VectorStore {
return &VectorStore{
mem:     mem,
vectors: make(map[string]arrow.Record),
}
}

// ListFlights returns available streams
func (s *VectorStore) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
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
return err
}
}
return nil
}

// GetFlightInfo returns metadata for a specific stream
func (s *VectorStore) GetFlightInfo(ctx flight.FlightServerContext, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
if len(desc.Path) == 0 {
return nil, fmt.Errorf("invalid path")
}
name := desc.Path[0]

s.mu.RLock()
rec, ok := s.vectors[name]
s.mu.RUnlock()

if !ok {
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

s.mu.RLock()
rec, ok := s.vectors[name]
s.mu.RUnlock()

if !ok {
return fmt.Errorf("vector not found: %s", name)
}

w := flight.NewRecordWriter(stream, flight.NewIpcPayloadWriter(stream))
defer w.Close()

w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{name}})
return w.Write(rec)
}

// DoPut accepts data from the client
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
r, err := flight.NewRecordReader(stream)
if err != nil {
return err
}
defer r.Release()

for r.Next() {
rec := r.Record()
rec.Retain()

// Simple storage: overwrite based on first path component
// In reality, you'd append or handle chunks
if len(r.FlightDescriptor().Path) > 0 {
name := r.FlightDescriptor().Path[0]
s.mu.Lock()
if old, exists := s.vectors[name]; exists {
old.Release()
}
s.vectors[name] = rec
s.mu.Unlock()
}
}
return nil
}
