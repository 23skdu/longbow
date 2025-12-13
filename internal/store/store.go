package store

import (
"context"
"encoding/json"
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
mem           memory.Allocator
logger        *slog.Logger
mu            sync.RWMutex
vectors       map[string][]arrow.Record
maxMemory     int64
currentMemory int64
}

func NewVectorStore(mem memory.Allocator, logger *slog.Logger, maxMemory int64) *VectorStore {
return &VectorStore{
mem:           mem,
logger:        logger,
vectors:       make(map[string][]arrow.Record),
maxMemory:     maxMemory,
currentMemory: 0,
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
return nil, fmt.Errorf("invalid path")
}
name := desc.Path[0]

s.mu.RLock()
recs, ok := s.vectors[name]
s.mu.RUnlock()

if !ok || len(recs) == 0 {
return nil, fmt.Errorf("vector not found: %s", name)
}

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

// TicketQuery defines the JSON structure for DoGet tickets
type TicketQuery struct {
Name  string `json:"name"`
Limit int64  `json:"limit"`
}

// DoGet streams data to the client with optional predicate pushdown
func (s *VectorStore) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
start := time.Now()
method := "DoGet"

// Parse Ticket
name := string(tkt.Ticket)
limit := int64(-1)

var query TicketQuery
if err := json.Unmarshal(tkt.Ticket, &query); err == nil && query.Name != "" {
name = query.Name
limit = query.Limit
}

s.logger.Info("DoGet called", "ticket", name, "limit", limit)

s.mu.RLock()
recs, ok := s.vectors[name]
s.mu.RUnlock()

if !ok {
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return fmt.Errorf("vector not found: %s", name)
}

if len(recs) == 0 {
return nil
}

w := flight.NewRecordWriter(stream, ipc.WithSchema(recs[0].Schema()))
defer w.Close()

w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{name}})

rowsSent := int64(0)
for _, rec := range recs {
if limit > 0 && rowsSent >= limit {
break
}

// If we need to slice the record to fit the limit
toWrite := rec
if limit > 0 && rowsSent+rec.NumRows() > limit {
remaining := limit - rowsSent
toWrite = rec.NewSlice(0, remaining)
defer toWrite.Release()
}

if err := w.Write(toWrite); err != nil {
s.logger.Error("Failed to write record", "error", err)
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}
rowsSent += toWrite.NumRows()
}

metrics.FlightOperationsTotal.WithLabelValues(method, "ok").Inc()
metrics.FlightDurationSeconds.WithLabelValues(method).Observe(time.Since(start).Seconds())
metrics.FlightBytesProcessed.WithLabelValues(method).Add(float64(rowsSent))

return nil
}

// DoPut accepts data from the client with memory limits
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
start := time.Now()
method := "DoPut"

r, err := flight.NewRecordReader(stream)
if err != nil {
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}
defer r.Release()

name := "default"
// Use LatestFlightDescriptor to get the descriptor from the stream
if desc := r.LatestFlightDescriptor(); desc != nil && len(desc.Path) > 0 {
name = desc.Path[0]
}

rowsWritten := 0

for r.Next() {
rec := r.Record()
rec.Retain()

size := calculateRecordSize(rec)

s.mu.Lock()
if s.maxMemory > 0 && s.currentMemory+size > s.maxMemory {
s.mu.Unlock()
rec.Release()
s.logger.Error("Memory limit exceeded", "current", s.currentMemory, "max", s.maxMemory, "needed", size)
return fmt.Errorf("resource exhausted: memory limit exceeded")
}
s.vectors[name] = append(s.vectors[name], rec)
s.currentMemory += size
s.mu.Unlock()

rowsWritten += int(rec.NumRows())
}

if r.Err() != nil {
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return r.Err()
}

metrics.FlightOperationsTotal.WithLabelValues(method, "ok").Inc()
metrics.FlightDurationSeconds.WithLabelValues(method).Observe(time.Since(start).Seconds())
metrics.FlightBytesProcessed.WithLabelValues(method).Add(float64(rowsWritten))

return nil
}

// DoAction handles management commands
func (s *VectorStore) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
s.logger.Info("DoAction called", "type", action.Type)

switch action.Type {
case "drop_dataset":
name := string(action.Body)
s.mu.Lock()
if recs, ok := s.vectors[name]; ok {
for _, r := range recs {
s.currentMemory -= calculateRecordSize(r)
r.Release()
}
delete(s.vectors, name)
}
s.mu.Unlock()
result, _ := json.Marshal(map[string]string{"status": "ok", "message": "dataset dropped"})
if err := stream.Send(&flight.Result{Body: result}); err != nil {
return err
}

case "get_stats":
s.mu.RLock()
stats := map[string]interface{}{
"datasets":       len(s.vectors),
"current_memory": s.currentMemory,
"max_memory":     s.maxMemory,
}
s.mu.RUnlock()
result, _ := json.Marshal(stats)
if err := stream.Send(&flight.Result{Body: result}); err != nil {
return err
}

case "force_snapshot":
// Placeholder for persistence logic
s.logger.Info("Snapshot requested (not implemented)")
result, _ := json.Marshal(map[string]string{"status": "skipped", "message": "persistence not enabled"})
if err := stream.Send(&flight.Result{Body: result}); err != nil {
return err
}

default:
return fmt.Errorf("unknown action: %s", action.Type)
}
return nil
}

func calculateRecordSize(rec arrow.Record) int64 {
size := int64(0)
for _, col := range rec.Columns() {
for _, buf := range col.Data().Buffers() {
if buf != nil {
size += int64(buf.Len())
}
}
}
return size
}
