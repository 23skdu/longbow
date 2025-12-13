package store

import (
"context"
"fmt"
"log/slog"
"sync"
"time"

"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/ipc"
"github.com/apache/arrow/go/v18/arrow/memory"
"github.com/23skdu/longbow/internal/metrics"
)

// VectorStore implements flight.FlightServer
type VectorStore struct {
flight.BaseFlightServer
mem     memory.Allocator
logger  *slog.Logger
mu      sync.RWMutex
vectors map[string][]arrow.Record // Changed to slice for append-only support
}

func NewVectorStore(mem memory.Allocator, logger *slog.Logger) *VectorStore {
return &VectorStore{
mem:     mem,
logger:  logger,
vectors: make(map[string][]arrow.Record),
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
recs, ok := s.vectors[name]
s.mu.RUnlock()

if !ok || len(recs) == 0 {
s.logger.Warn("Vector not found", "name", name)
return nil, fmt.Errorf("vector not found: %s", name)
}

// Use schema from the first record
schema := recs[0].Schema()
totalRows := int64(0)
for _, r := range recs {
totalRows += r.NumRows()
}

return &flight.FlightInfo{
Schema:           flight.SerializeSchema(schema, s.mem),
FlightDescriptor: desc,
TotalRecords:     totalRows,
TotalBytes:       -1,
}, nil
}

// DoGet streams data to the client
func (s *VectorStore) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
start := time.Now()
method := "DoGet"
name := string(tkt.Ticket)
s.logger.Info("DoGet called", "ticket", name)

s.mu.RLock()
recs, ok := s.vectors[name]
s.mu.RUnlock()

if !ok {
s.logger.Warn("Vector not found for ticket", "ticket", name)
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return fmt.Errorf("vector not found: %s", name)
}

if len(recs) == 0 {
return nil
}

// Use flight.NewRecordWriter directly with the stream
w := flight.NewRecordWriter(stream, ipc.WithSchema(recs[0].Schema()))
defer w.Close()

w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{name}})

rowsSent := 0
for _, rec := range recs {
if err := w.Write(rec); err != nil {
s.logger.Error("Failed to write record", "error", err, "ticket", name)
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}
rowsSent += int(rec.NumRows())
}

metrics.FlightOperationsTotal.WithLabelValues(method, "ok").Inc()
metrics.FlightDurationSeconds.WithLabelValues(method).Observe(time.Since(start).Seconds())
metrics.FlightBytesProcessed.WithLabelValues(method).Add(float64(rowsSent))

return nil
}

// DoPut accepts data from the client
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
start := time.Now()
method := "DoPut"

r, err := flight.NewRecordReader(stream)
if err != nil {
s.logger.Error("Failed to create record reader", "error", err)
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}
defer r.Release()

// Extract dataset name from descriptor if available
name := "default"
if r.FlightDescriptor() != nil && len(r.FlightDescriptor().Path) > 0 {
name = r.FlightDescriptor().Path[0]
}

rowsWritten := 0

for r.Next() {
rec := r.Record()
rec.Retain()

s.logger.Info("Storing vector batch", "name", name, "rows", rec.NumRows())

s.mu.Lock()
s.vectors[name] = append(s.vectors[name], rec)
s.mu.Unlock()

rowsWritten += int(rec.NumRows())
}

if r.Err() != nil {
s.logger.Error("Error reading stream", "error", r.Err())
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return r.Err()
}

metrics.FlightOperationsTotal.WithLabelValues(method, "ok").Inc()
metrics.FlightDurationSeconds.WithLabelValues(method).Observe(time.Since(start).Seconds())
metrics.FlightBytesProcessed.WithLabelValues(method).Add(float64(rowsWritten))

return nil
}
