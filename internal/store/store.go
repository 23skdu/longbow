package store

import (
	"fmt"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"math"
	"runtime"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"

	"github.com/apache/arrow/go/v18/arrow/compute"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/scalar")


// Dataset wraps records with metadata for eviction
type Dataset struct {
	Index *HNSWIndex
	Index *HNSWIndex
Records []arrow.Record
lastAccess int64 // UnixNano
Version int64
}

func (d *Dataset) LastAccess() time.Time {
return time.Unix(0, atomic.LoadInt64(&d.lastAccess))
}

func (d *Dataset) SetLastAccess(t time.Time) {
atomic.StoreInt64(&d.lastAccess, t.UnixNano())
}

// VectorStore implements flight.FlightServer
type VectorStore struct {
	flight.BaseFlightServer
	mem           memory.Allocator
	logger        *slog.Logger
	mu            sync.RWMutex
	vectors map[string]*Dataset
	maxMemory     int64
	currentMemory int64

	// Persistence
	dataPath      string
	walFile       *os.File
	walMu         sync.Mutex
	snapshotReset chan time.Duration
	ttlDuration time.Duration
}

func NewVectorStore(mem memory.Allocator, logger *slog.Logger, maxMemory int64, ttl time.Duration) *VectorStore {
s := &VectorStore{
mem: mem,
logger: logger,
vectors: make(map[string]*Dataset),
maxMemory: maxMemory,
ttlDuration: ttl,
currentMemory: 0,
snapshotReset: make(chan time.Duration, 1),
}
	s.StartMetricsTicker(10 * time.Second)
	return s
}

// UpdateConfig updates the dynamic configuration of the store
func (s *VectorStore) UpdateConfig(maxMemory int64, snapshotInterval time.Duration) {
	s.mu.Lock()
	s.maxMemory = maxMemory
	s.mu.Unlock()

	s.logger.Info("Store configuration updated", "max_memory", maxMemory)

	// Non-blocking send to reset channel
	select {
	case s.snapshotReset <- snapshotInterval:
		s.logger.Info("Snapshot interval update signal sent", "new_interval", snapshotInterval)
	default:
		// If channel is full, drain and replace (last write wins for config)
		select {
		case <-s.snapshotReset:
		default:
		}
		s.snapshotReset <- snapshotInterval
		s.logger.Info("Snapshot interval update signal sent (drained previous)", "new_interval", snapshotInterval)
	}
}

// TicketQuery defines the JSON structure for DoGet tickets and ListFlights criteria
type TicketQuery struct {
	Name    string   `json:"name"`
	Limit   int64    `json:"limit"`
	Filters []Filter `json:"filters"`
}

// Filter defines a predicate for filtering streams
type Filter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

// applyFilter evaluates filters against stream metadata
func (s *VectorStore) applyFilter(name string, recs []arrow.Record, filters []Filter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		switch f.Field {
		case "name":
			switch f.Operator {
			case "=":
				if name != f.Value {
					return false
				}
			case "!=":
				if name == f.Value {
					return false
				}
			case "contains":
				if !strings.Contains(name, f.Value) {
					return false
				}
			}
		case "rows":
			// Calculate total rows
			totalRows := int64(0)
			for _, r := range recs {
				totalRows += r.NumRows()
			}
			val, err := strconv.ParseInt(f.Value, 10, 64)
			if err != nil {
				continue // Skip invalid filter values
			}
			switch f.Operator {
			case "=":
				if totalRows != val {
					return false
				}
			case ">":
				if totalRows <= val {
					return false
				}
			case "<":
				if totalRows >= val {
					return false
				}
			case ">=":
				if totalRows < val {
					return false
				}
			case "<=":
				if totalRows > val {
					return false
				}
			}
		}
	}
	return true
}

// ListFlights returns available streams
func (s *VectorStore) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
s.logger.Info("ListFlights called")

var query TicketQuery
if c != nil && len(c.Expression) > 0 {
if err := json.Unmarshal(c.Expression, &query); err != nil {
s.logger.Warn("Failed to parse criteria expression", "error", err)
}
}

// Optimization: Collect info under lock, send outside lock
// This prevents slow clients from blocking write operations (DoPut)
var infos []*flight.FlightInfo

s.mu.RLock()
for name, ds := range s.vectors {
		recs := ds.Records
if !s.applyFilter(name, recs, query.Filters) {
continue
}

info := &flight.FlightInfo{
FlightDescriptor: &flight.FlightDescriptor{
Type: flight.DescriptorPATH,
Path: []string{name},
},
}
infos = append(infos, info)
}
s.mu.RUnlock()

// Send stream messages without holding the lock
for _, info := range infos {
if err := stream.Send(info); err != nil {
name := ""
if len(info.FlightDescriptor.Path) > 0 {
name = info.FlightDescriptor.Path[0]
}
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
ds, ok := s.vectors[name]
s.mu.RUnlock()

if !ok || len(ds.Records) == 0 {
return nil, fmt.Errorf("vector not found: %s", name)
}
recs := ds.Records

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

s.mu.RLock() // Read lock is sufficient with atomic LastAccess
	ds, ok := s.vectors[name]
	if ok {
		ds.SetLastAccess(time.Now())
	}
	s.mu.RUnlock()

recs := ds.Records

if !ok {
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return fmt.Errorf("vector not found: %s", name)
}

if len(recs) == 0 {
return nil
}

w := flight.NewRecordWriter(stream, ipc.WithSchema(recs[0].Schema()), ipc.WithLZ4())
defer w.Close()

w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{name}})

rowsSent := int64(0)
for _, rec := range recs {
if limit > 0 && rowsSent >= limit {
break
}

// Apply Filtering using arrow/compute
filteredRec, err := s.filterRecord(stream.Context(), rec, query.Filters)
if err != nil {
s.logger.Error("Filtering failed", "error", err)
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}

if filteredRec.NumRows() == 0 {
filteredRec.Release()
continue
}

toWrite := filteredRec
sliced := false
if limit > 0 && rowsSent+filteredRec.NumRows() > limit {
remaining := limit - rowsSent
toWrite = filteredRec.NewSlice(0, remaining)
sliced = true
}

if err := w.Write(toWrite); err != nil {
if sliced { toWrite.Release() }
filteredRec.Release()
s.logger.Error("Failed to write record", "error", err)
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}
rowsSent += toWrite.NumRows()

if sliced { toWrite.Release() }
filteredRec.Release()
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

	// Schema Validation
	s.mu.RLock()
	if ds, ok := s.vectors[name]; ok && len(ds.Records) > 0 {
		existingRecs := ds.Records
		existingSchema := existingRecs[0].Schema()
		if !existingSchema.Equal(r.Schema()) {
// Schema Evolution: Check if new schema is compatible (superset)
// For now, we allow adding new nullable columns.
if len(r.Schema().Fields()) > len(existingSchema.Fields()) {
// Check if prefix matches
compatible := true
for i, f := range existingSchema.Fields() {
if !f.Equal(r.Schema().Field(i)) {
compatible = false
break
}
}
if compatible {
// Upgrade Schema: Accept new record, increment version
s.mu.RUnlock()
s.mu.Lock()
s.vectors[name].Version++
s.logger.Info("Schema evolved", "name", name, "version", s.vectors[name].Version)
s.mu.Unlock()
s.mu.RLock()
} else {
s.mu.RUnlock()
return fmt.Errorf("schema mismatch: incompatible schema evolution for dataset '%s'", name)
}
} else {
s.mu.RUnlock()
return fmt.Errorf("schema mismatch: incoming schema does not match existing dataset '%s'", name)
}
}
	}
	s.mu.RUnlock()

	rowsWritten := 0

	for r.Next() {
 		rawRec := r.Record()
		rec, err := s.ensureTimestamp(rawRec)
		if err != nil {
			metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
			return fmt.Errorf("failed to ensure timestamp: %w", err)
		}
		size := calculateRecordSize(rec)

		s.mu.Lock()
		if s.maxMemory > 0 && s.currentMemory+size > s.maxMemory {
			s.mu.Unlock()
			rec.Release()
			s.logger.Error("Memory limit exceeded", "current", s.currentMemory, "max", s.maxMemory, "needed", size)
			return fmt.Errorf("resource exhausted: memory limit exceeded")
		}
		if _, ok := s.vectors[name]; !ok {
			s.vectors[name] = &Dataset{Records: []arrow.Record{}, lastAccess: time.Now().UnixNano()}
		}
		s.vectors[name].Records = append(s.vectors[name].Records, rec)
		s.vectors[name].SetLastAccess(time.Now())
		s.currentMemory += size
		s.mu.Unlock()

		buildStart := time.Now()
		// Write to WAL
if err := s.writeToWAL(rec, name); err != nil {
s.logger.Error("Failed to write to WAL", "error", err)
// Strict Durability: Fail the request if persistence fails
return fmt.Errorf("persistence failed: %w", err)
}

		rowsWritten += int(rec.NumRows())
		metrics.IndexBuildLatency.Observe(time.Since(buildStart).Seconds())
		s.updateVectorMetrics(rec)
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
		if ds, ok := s.vectors[name]; ok {
			recs := ds.Records
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
		if err := s.Snapshot(); err != nil {
			result, _ := json.Marshal(map[string]string{"status": "error", "message": err.Error()})
			if err := stream.Send(&flight.Result{Body: result}); err != nil {
				return err
			}
			return err
		}
		result, _ := json.Marshal(map[string]string{"status": "ok", "message": "snapshot created"})
		if err := stream.Send(&flight.Result{Body: result}); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown action: %s", action.Type)
	}
	return nil
}

func calculateRecordSize(rec arrow.Record) int64 {
	if rec == nil {
		return 0
	}
	size := int64(0)
	for _, col := range rec.Columns() {
		if col == nil || col.Data() == nil {
			continue
		}
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
	}
	return size
}


// filterRecord applies filters using arrow/compute
func (s *VectorStore) filterRecord(ctx context.Context, rec arrow.Record, filters []Filter) (arrow.Record, error) {
if len(filters) == 0 {
rec.Retain()
return rec, nil
}

var mask *array.Boolean

for _, f := range filters {
indices := rec.Schema().FieldIndices(f.Field)
if len(indices) == 0 {
continue
}
colIdx := indices[0]
col := rec.Column(colIdx)

var valScalar scalar.Scalar
switch col.DataType().ID() {
case arrow.STRING:
valScalar = scalar.NewStringScalar(f.Value)
case arrow.INT64:
v, err := strconv.ParseInt(f.Value, 10, 64)
if err != nil {
continue
}
valScalar = scalar.NewInt64Scalar(v)
 		case arrow.TIMESTAMP:
			t, err := time.Parse(time.RFC3339, f.Value)
			if err != nil {
				continue
			}
			ts, _ := arrow.TimestampFromTime(t, col.DataType().(*arrow.TimestampType).Unit)
			valScalar = scalar.NewTimestampScalar(ts, col.DataType().(*arrow.TimestampType))
case arrow.FLOAT64:
v, err := strconv.ParseFloat(f.Value, 64)
if err != nil {
continue
}
valScalar = scalar.NewFloat64Scalar(v)
default:
continue
}

var fn string
switch f.Operator {
case "=": fn = "equal"
case "!=": fn = "not_equal"
case ">": fn = "greater"
case "<": fn = "less"
case ">=": fn = "greater_equal"
case "<=": fn = "less_equal"
default:
continue
}

args := []compute.Datum{
compute.NewDatum(col.Data()),
compute.NewDatum(valScalar),
}
result, err := compute.CallFunction(ctx, fn, nil, args...)
if err != nil {
return nil, fmt.Errorf("compute error on field %s: %w", f.Field, err)
}

resultArr := result.(*compute.ArrayDatum).MakeArray().(*array.Boolean)

if mask == nil {
mask = resultArr
} else {
andRes, err := compute.CallFunction(ctx, "and", nil, compute.NewDatum(mask.Data()), compute.NewDatum(resultArr.Data()))
mask.Release()
resultArr.Release()
if err != nil {
return nil, err
}
mask = andRes.(*compute.ArrayDatum).MakeArray().(*array.Boolean)
}
}

if mask == nil {
rec.Retain()
return rec, nil
}
defer mask.Release()

filterRes, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(rec), compute.NewDatum(mask.Data()))
if err != nil {
return nil, err
}
return filterRes.(*compute.RecordDatum).Value, nil
}


// StartEvictionTicker starts the background eviction loop
func (s *VectorStore) StartEvictionTicker(interval time.Duration) {
ticker := time.NewTicker(interval)
go func() {
for range ticker.C {
s.evictTTL()
}
}()
}

// evictTTL removes datasets that haven't been accessed within the TTL duration
func (s *VectorStore) evictTTL() {
if s.ttlDuration <= 0 {
return
}

s.mu.Lock()
defer s.mu.Unlock()

now := time.Now()
evictedCount := 0

for name, ds := range s.vectors {
if now.Sub(ds.LastAccess()) > s.ttlDuration {
// Evict
for _, r := range ds.Records {
s.currentMemory -= calculateRecordSize(r)
r.Release()
}
delete(s.vectors, name)
evictedCount++
s.logger.Info("Evicted dataset due to TTL", "name", name)
metrics.EvictionsTotal.WithLabelValues("ttl").Inc()
}
}
if evictedCount > 0 {
s.logger.Info("TTL eviction completed", "evicted_count", evictedCount)
}
}

// evictLRU removes the least recently used datasets until enough memory is freed
func (s *VectorStore) evictLRU(needed int64) error {
// Assumes s.mu is already locked by the caller

for s.maxMemory > 0 && s.currentMemory+needed > s.maxMemory {
var oldestName string
var oldestTime time.Time
first := true

for name, ds := range s.vectors {
if first || ds.LastAccess().Before(oldestTime) {
oldestName = name
oldestTime = ds.LastAccess()
first = false
}
}

if first {
// No datasets to evict
return fmt.Errorf("resource exhausted: memory limit exceeded and no datasets to evict")
}

// Evict oldest
if ds, ok := s.vectors[oldestName]; ok {
for _, r := range ds.Records {
s.currentMemory -= calculateRecordSize(r)
r.Release()
}
delete(s.vectors, oldestName)
s.logger.Info("Evicted dataset due to LRU", "name", oldestName, "freed", calculateDatasetSize(ds))
metrics.EvictionsTotal.WithLabelValues("lru").Inc()
}
}
return nil
}

func calculateDatasetSize(ds *Dataset) int64 {
size := int64(0)
for _, r := range ds.Records {
size += calculateRecordSize(r)
}
return size
}

// ensureTimestamp ensures the record has a timestamp column, adding one if missing
func (s *VectorStore) ensureTimestamp(rec arrow.Record) (arrow.Record, error) {
if rec.Schema().HasField("timestamp") {
rec.Retain()
return rec, nil
}

// Create new schema
fields := rec.Schema().Fields()
tsField := arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false}
newFields := append(fields, tsField)
meta := rec.Schema().Metadata()
newSchema := arrow.NewSchema(newFields, &meta)

// Create timestamp column
bldr := array.NewTimestampBuilder(s.mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
defer bldr.Release()

now := time.Now()
ts, _ := arrow.TimestampFromTime(now, arrow.Nanosecond)

for i := 0; i < int(rec.NumRows()); i++ {
bldr.Append(ts)
}
tsArr := bldr.NewArray()
defer tsArr.Release()

// Build new columns
cols := make([]arrow.Array, len(fields)+1)
for i, col := range rec.Columns() {
cols[i] = col
}
cols[len(fields)] = tsArr

newRec := array.NewRecord(newSchema, cols, rec.NumRows())
return newRec, nil
}


// StartMetricsTicker starts background metrics collection
func (s *VectorStore) StartMetricsTicker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			s.updateMemoryMetrics()
		}
	}()
}

func (s *VectorStore) updateMemoryMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	if m.Sys > 0 {
		ratio := float64(m.HeapAlloc) / float64(m.Sys)
		metrics.MemoryFragmentationRatio.Set(ratio)
	}
}

func (s *VectorStore) updateVectorMetrics(rec arrow.Record) {
	metrics.VectorIndexSize.Add(float64(rec.NumRows()))
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			col := rec.Column(i)
			avgNorm := calculateBatchNorm(col)
			metrics.AverageVectorNorm.Set(avgNorm)
			break
		}
	}
}


func calculateBatchNorm(arr arrow.Array) float64 {
	listArr, ok := arr.(*array.FixedSizeList)
	if !ok {
		return 0
	}

	// Get list size from type
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())

	// Access values via child data
	if len(listArr.Data().Children()) == 0 {
		return 0
	}
	valsData := listArr.Data().Children()[0]
	
	// Create a Float32 array wrapper to access values
	floatArr := array.NewFloat32Data(valsData)
	defer floatArr.Release()
	
	var totalNorm float64
	count := 0
	
	for i := 0; i < listArr.Len(); i++ {
		start := i * width
		end := start + width
		
		if end > floatArr.Len() {
			break
		}
		
		var sumSq float64
		for j := start; j < end; j++ {
			val := floatArr.Value(j)
			sumSq += float64(val * val)
		}
		totalNorm += math.Sqrt(sumSq)
		count++
	}
	
	if count == 0 {
		return 0
	}
	return totalNorm / float64(count)
}
