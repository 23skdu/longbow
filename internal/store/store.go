package store

import (
"context"
"encoding/json"
"fmt"
"go.uber.org/zap"
"math"
"os"
"path/filepath"
"runtime"
"strconv"
"strings"
"sync"
"sync/atomic"
"time"

"github.com/23skdu/longbow/internal/metrics"
"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/compute"
"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/ipc"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/apache/arrow-go/v18/arrow/scalar"
)

// zeroAllocTicketParser is a package-level parser for DoGet hot path
var zeroAllocTicketParser = NewZeroAllocTicketParser()

type IndexJob struct {
	DatasetName string
	BatchIdx    int
	RowIdx      int
CreatedAt time.Time
}

// Dataset wraps records with metadata for eviction
type Dataset struct {
Records    []arrow.RecordBatch
lastAccess int64 // UnixNano
Version    int64
	Index        *HNSWIndex
	shardedIndex *ShardedHNSW
mu sync.RWMutex
	Name string
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
logger        *zap.Logger
vectors *ShardedMap
maxMemory atomic.Int64
currentMemory atomic.Int64
maxWALSize atomic.Int64

// Persistence
dataPath      string
walFile       *os.File
walMu         sync.Mutex
walBatcher *WALBatcher
snapshotReset chan time.Duration
ttlDuration   time.Duration
	indexQueue *IndexJobQueue
	// Column-based inverted index for O(1) equality filter lookups
	columnIndex *ColumnInvertedIndex
	indexedColumns []string // columns to index for fast equality lookups
	metadata *COWMetadataMap // COW metadata map for lock-free ListFlights

// Shutdown coordination
shutdownState int32
stopChan      chan struct{}
workerWg      sync.WaitGroup
indexWg       sync.WaitGroup
// Compaction subsystem
compactionConfig CompactionConfig
compactionWorker *CompactionWorker
	// Hybrid search (BM25 + Vector)
	hybridSearchConfig HybridSearchConfig
	bm25Index          *BM25InvertedIndex
	// Request concurrency limiter
	semaphore *RequestSemaphore
    // Replication subsystem
	flightClientPool  *FlightClientPool
	replicationConfig ReplicationConfig
	replicationHook   func(ctx context.Context, dataset string, records []arrow.RecordBatch)
	// DoGet pipeline subsystem
	doGetPipelinePool *DoGetPipelinePool
	pipelineThreshold int
	// Multi-tenancy namespace manager
	nsManager *namespaceManager
}

func NewVectorStore(mem memory.Allocator, logger *zap.Logger, maxMemory, maxWALSize int64, ttl time.Duration) *VectorStore {
s := &VectorStore{
mem:           mem,
logger:        logger,
vectors: NewShardedMap(),
ttlDuration:   ttl,
snapshotReset: make(chan time.Duration, 1),
		indexQueue: NewIndexJobQueue(DefaultIndexJobQueueConfig()),
		stopChan:      make(chan struct{}),
		columnIndex: NewColumnInvertedIndex(),
metadata: NewCOWMetadataMap(),
		semaphore: NewRequestSemaphore(DefaultRequestSemaphoreConfig()),
		nsManager: newNamespaceManager(),
	}
	s.maxMemory.Store(maxMemory)
	s.maxWALSize.Store(maxWALSize)
	s.startIndexingWorkers(runtime.NumCPU())
s.StartMetricsTicker(10 * time.Second)
if maxWALSize > 0 {
s.StartWALCheckTicker(1 * time.Minute)
}
s.initCompaction(DefaultCompactionConfig())
return s
}

// NewVectorStoreWithSemaphore creates a VectorStore with custom semaphore configuration.
// Use this for fine-grained control over concurrent request limiting.
func NewVectorStoreWithSemaphore(mem memory.Allocator, logger *zap.Logger, maxMemory, maxWALSize int64, ttl time.Duration, semCfg RequestSemaphoreConfig) *VectorStore {
s := &VectorStore{
mem:           mem,
logger:        logger,
vectors:       NewShardedMap(),
ttlDuration:   ttl,
snapshotReset: make(chan time.Duration, 1),
indexQueue:    NewIndexJobQueue(DefaultIndexJobQueueConfig()),
stopChan:      make(chan struct{}),
columnIndex:   NewColumnInvertedIndex(),
metadata:      NewCOWMetadataMap(),
semaphore:     NewRequestSemaphore(semCfg),
		nsManager: newNamespaceManager(),
}
s.maxMemory.Store(maxMemory)
s.maxWALSize.Store(maxWALSize)
s.startIndexingWorkers(runtime.NumCPU())
s.StartMetricsTicker(10 * time.Second)
if maxWALSize > 0 {
s.StartWALCheckTicker(1 * time.Minute)
}
s.initCompaction(DefaultCompactionConfig())
return s
}

// NewVectorStoreWithCompaction creates a VectorStore with custom compaction configuration.
// Use this to configure auto-compaction threshold and timing.
func NewVectorStoreWithCompaction(mem memory.Allocator, logger *zap.Logger, maxMemory, maxWALSize int64, ttl time.Duration, compactCfg CompactionConfig) *VectorStore {
s := &VectorStore{
mem:           mem,
logger:        logger,
vectors:       NewShardedMap(),
ttlDuration:   ttl,
snapshotReset: make(chan time.Duration, 1),
indexQueue:    NewIndexJobQueue(DefaultIndexJobQueueConfig()),
stopChan:      make(chan struct{}),
columnIndex:   NewColumnInvertedIndex(),
metadata:      NewCOWMetadataMap(),
semaphore:     NewRequestSemaphore(DefaultRequestSemaphoreConfig()),
		nsManager: newNamespaceManager(),
}
s.maxMemory.Store(maxMemory)
s.maxWALSize.Store(maxWALSize)
s.startIndexingWorkers(runtime.NumCPU())
s.StartMetricsTicker(10 * time.Second)
if maxWALSize > 0 {
s.StartWALCheckTicker(1 * time.Minute)
}
s.initCompaction(compactCfg)
return s
}

// GetAutoCompactionTriggerCount returns the number of auto-triggered compactions.
func (s *VectorStore) GetAutoCompactionTriggerCount() int64 {
if s.compactionWorker == nil {
return 0
}
return s.compactionWorker.GetTriggerCount()
}

// UpdateConfig updates the dynamic configuration of the store
func (s *VectorStore) UpdateConfig(maxMemory, maxWALSize int64, snapshotInterval time.Duration) {
s.maxMemory.Store(maxMemory)
s.maxWALSize.Store(maxWALSize)

s.logger.Info("Store configuration updated", zap.Any("max_memory", maxMemory), zap.Any("max_wal_size", maxWALSize))

// Non-blocking send to reset channel
select {
case s.snapshotReset <- snapshotInterval:
s.logger.Info("Snapshot interval update signal sent", zap.Any("new_interval", snapshotInterval))
default:
// If channel is full, drain and replace (last write wins for config)
select {
case <-s.snapshotReset:
default:
}
s.snapshotReset <- snapshotInterval
s.logger.Info("Snapshot interval update signal sent (drained previous)", zap.Any("new_interval", snapshotInterval))
}
}

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

// applyMetadataFilter evaluates filters using COW metadata (lock-free)
func (s *VectorStore) applyMetadataFilter(name string, meta DatasetMetadata, filters []Filter) bool {
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
// Use pre-calculated TotalRows from metadata - O(1)
totalRows := meta.TotalRows
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
s.logger.Warn("Failed to parse criteria expression", zap.Error(err))
}
}

// Lock-free iteration using COW metadata snapshot
// This eliminates per-dataset RLock contention entirely
metaSnapshot := s.metadata.Snapshot()
infos := make([]*flight.FlightInfo, 0, len(metaSnapshot))
for name, meta := range metaSnapshot {
	if !s.applyMetadataFilter(name, meta, query.Filters) {
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

// Send stream messages without holding the lock
for _, info := range infos {
if err := stream.Send(info); err != nil {
name := ""
if len(info.FlightDescriptor.Path) > 0 {
name = info.FlightDescriptor.Path[0]
}
s.logger.Error("Failed to send flight info", zap.Error(err), zap.Any("name", name))
return err
}
}
return nil
}

// GetFlightInfo returns metadata for a specific stream
func (s *VectorStore) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(desc.Path) == 0 {
		return nil, NewInvalidArgumentError("path", "must not be empty")
	}
	name := desc.Path[0]

ds, ok := s.vectors.Get(name)

if !ok || len(ds.Records) == 0 {
return nil, NewNotFoundError("dataset", name)
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
	// Acquire semaphore - limit concurrent requests to prevent thread thrashing
	if err := s.semaphore.Acquire(stream.Context()); err != nil {
		return err
	}
	defer s.semaphore.Release()

	start := time.Now()
// Track active search contexts
metrics.ActiveSearchContexts.Inc()
defer metrics.ActiveSearchContexts.Dec()
method := "DoGet"

// Parse Ticket
name := string(tkt.Ticket)
limit := int64(-1)

// Track ticket parsing duration - use zero-alloc parser
ticketParseStart := time.Now()
query, parseErr := zeroAllocTicketParser.Parse(tkt.Ticket)
if parseErr != nil {
// Fallback to standard parser on error
query, parseErr = FastParseTicketQuery(tkt.Ticket)
metrics.TicketParseFallbackTotal.Inc()
} else {
metrics.ZeroAllocTicketParseTotal.Inc()
}
metrics.FlightTicketParseDurationSeconds.Observe(time.Since(ticketParseStart).Seconds())
if parseErr == nil && query.Name != "" {
name = query.Name
limit = query.Limit
}

s.logger.Info("DoGet called", zap.Any("ticket", name), zap.Int64("limit", limit))

ds, ok := s.vectors.Get(name)
if !ok {
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return NewNotFoundError("dataset", name)
}

ds.SetLastAccess(time.Now())
recs := ds.Records

if len(recs) == 0 {
return nil
}

w := flight.NewRecordWriter(stream, ipc.WithSchema(recs[0].Schema()), ipc.WithLZ4())
defer func() { _ = w.Close() }()

w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{name}})

// Use pipeline for multi-batch datasets (parallel processing)
if s.shouldUsePipeline(len(recs)) {
rowsSent, err := s.doGetWithPipeline(stream.Context(), name, recs, &query, w, limit)
if err != nil {
s.logger.Error("Pipeline processing failed", zap.Error(err))
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}
metrics.FlightOperationsTotal.WithLabelValues(method, "ok").Inc()
metrics.FlightDurationSeconds.WithLabelValues(method).Observe(time.Since(start).Seconds())
metrics.FlightBytesProcessed.WithLabelValues(method).Add(float64(rowsSent))
return nil
}

// Fallback: serial processing for small datasets
rowsSent := int64(0)
for batchIdx, rec := range recs {
if limit > 0 && rowsSent >= limit {
break
}

// Apply Filtering using arrow/compute
// Track filter execution time
filterStart := time.Now()
inputRows := rec.NumRows()
filteredRec, err := s.filterRecordOptimized(stream.Context(), name, rec, batchIdx, query.Filters)
if err != nil {
s.logger.Error("Filtering failed", zap.Error(err))
metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
return err
}
// Record filter execution duration and selectivity
metrics.FilterExecutionDurationSeconds.WithLabelValues(name).Observe(time.Since(filterStart).Seconds())
if inputRows > 0 {
metrics.FilterSelectivityRatio.WithLabelValues(name).Observe(float64(filteredRec.NumRows()) / float64(inputRows))
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
s.logger.Error("Failed to write record", zap.Error(err))
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
	// Acquire semaphore - limit concurrent requests to prevent thread thrashing
	if err := s.semaphore.Acquire(stream.Context()); err != nil {
		return err
	}
	defer s.semaphore.Release()

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

	// Schema Validation with Lock Granularity:
	// Use RLock for initial schema check (read-only), only upgrade to Lock if schema evolution needed.
	// This reduces contention on the hot write path.
	if ds, ok := s.vectors.Get(name); ok {
		existingSchema := ds.GetExistingSchema() // Uses RLock internally
		if existingSchema != nil {
			compat := CheckSchemaCompatibility(existingSchema, r.Schema())
			switch compat {
			case SchemaExactMatch:
				// Schema matches, proceed without any write lock
			case SchemaEvolution:
				// Schema evolved - upgrade to write lock only for version increment
				ds.UpgradeSchemaVersion() // Uses Lock internally
				s.logger.Info("Schema evolved", zap.Any("name", name), zap.Any("version", ds.GetVersion()))
			case SchemaIncompatible:
				return NewSchemaMismatchError(name, "incompatible schema: incoming schema does not match existing")
			}
		}
	}

	rowsWritten := 0

	for r.Next() {
 		rawRec := r.RecordBatch()
		rec, err := s.ensureTimestamp(rawRec)
		if err != nil {
			metrics.FlightOperationsTotal.WithLabelValues(method, "error").Inc()
			return NewInternalError("ensure timestamp", err)
		}
		size := CachedRecordSize(rec)

		// Lock-free memory limit check with CAS loop
		maxMem := s.maxMemory.Load()
		if maxMem > 0 {
			for {
				current := s.currentMemory.Load()
				if current+size > maxMem {
					rec.Release()
					s.logger.Error("Memory limit exceeded", zap.Any("current", current), zap.Any("max", maxMem), zap.Any("needed", size))
					return NewResourceExhaustedError("memory", "limit exceeded")
				}
				if s.currentMemory.CompareAndSwap(current, current+size) {
					break
				}
			}
		} else {
			s.currentMemory.Add(size)
		}
// Track Arrow memory usage
metrics.ArrowMemoryUsedBytes.WithLabelValues("default").Set(float64(s.currentMemory.Load()))

		ds := s.vectors.GetOrCreate(name, func() *Dataset {
			newDs := &Dataset{Records: []arrow.RecordBatch{}, lastAccess: time.Now().UnixNano()}
			newDs.Index = NewHNSWIndex(newDs)
			return newDs
		})
// Track lock wait duration
lockWaitStart := time.Now()
dsLockStart1 := time.Now()
ds.mu.Lock()
metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart1).Seconds())
metrics.DatasetLockWaitDurationSeconds.WithLabelValues("write").Observe(time.Since(lockWaitStart).Seconds())
batchIdx := len(ds.Records)
ds.Records = append(ds.Records, rec)
ds.mu.Unlock()


// Auto-trigger compaction if batch count exceeds threshold
if s.compactionWorker != nil && batchIdx+1 > s.compactionConfig.MinBatchesToCompact {
_ = s.compactionWorker.TriggerCompaction(name)
}
// Track RecordBatch count (fragmentation indicator)
metrics.DatasetRecordBatchesCount.WithLabelValues(name).Set(float64(len(ds.Records)))
// Update COW metadata for lock-free ListFlights
if _, exists := s.metadata.Get(name); !exists {
s.metadata.Set(name, DatasetMetadata{
Name:       name,
Schema:     rec.Schema(),
TotalRows:  rec.NumRows(),
BatchCount: 1,
})
} else {
s.metadata.IncrementStats(name, rec.NumRows(), 1)
}

// Index columns for fast equality lookups
s.IndexRecordColumns(name, rec, batchIdx)

ds.SetLastAccess(time.Now())

	// Write to WAL
	if s.walBatcher != nil {
		if err := s.walBatcher.Write(rec, name); err != nil {
			s.logger.Error("Failed to write to WAL", zap.Error(err))
			// Strict Durability: Fail the request if persistence fails
			return NewPersistenceError("WAL write", err)
		}
	}

	rowsWritten += int(rec.NumRows())
// Async Indexing
numRows := int(rec.NumRows())
for i := 0; i < numRows; i++ {
s.indexQueue.Send(IndexJob{
DatasetName: name,
BatchIdx:    batchIdx,
RowIdx:      i,
CreatedAt: time.Now(),
})
}

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
	s.logger.Info("DoAction called", zap.Any("type", action.Type))

	switch action.Type {
	case "drop_dataset":
		name := string(action.Body)
		if ds, ok := s.vectors.Get(name); ok {
dsLockStart2 := time.Now()
ds.mu.Lock()
metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart2).Seconds())
var freedMem int64
for _, r := range ds.Records {
freedMem += CachedRecordSize(r)
r.Release()
}
ds.Records = nil // Clear references
ds.mu.Unlock()
s.currentMemory.Add(-freedMem)
s.vectors.Delete(name)
}
		result, _ := json.Marshal(map[string]string{"status": "ok", "message": "dataset dropped"})
		if err := stream.Send(&flight.Result{Body: result}); err != nil {
			return err
		}

	case "get_stats":
		stats := map[string]interface{}{
			"datasets":       s.vectors.Len(),
			"current_memory": s.currentMemory.Load(),
			"max_memory": s.maxMemory.Load(),
		}
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

func calculateRecordSize(rec arrow.RecordBatch) int64 {
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
func (s *VectorStore) filterRecord(ctx context.Context, rec arrow.RecordBatch, filters []Filter) (arrow.RecordBatch, error) {
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

now := time.Now()
evictedCount := 0

// Collect keys to evict first (avoid deadlock - can't delete during Range)
var toEvict []string
s.vectors.Range(func(name string, ds *Dataset) bool {
if now.Sub(ds.LastAccess()) > s.ttlDuration {
toEvict = append(toEvict, name)
}
return true
})

// Now delete outside of Range
for _, name := range toEvict {
ds, ok := s.vectors.Get(name)
if !ok {
continue
}
dsLockStart3 := time.Now()
ds.mu.Lock()
metrics.DatasetLockWaitDurationSeconds.WithLabelValues("append").Observe(time.Since(dsLockStart3).Seconds())
var freedMem int64
for _, r := range ds.Records {
freedMem += CachedRecordSize(r)
r.Release()
}
ds.Records = nil
ds.mu.Unlock()
s.currentMemory.Add(-freedMem)
s.vectors.Delete(name)
evictedCount++
s.logger.Info("Evicted dataset due to TTL", zap.Any("name", name))
metrics.EvictionsTotal.WithLabelValues("ttl").Inc()
}
if evictedCount > 0 {
s.logger.Info("TTL eviction completed", zap.Any("evicted_count", evictedCount))
}
}

// evictLRU removes the least recently used datasets until enough memory is freed
func (s *VectorStore) evictLRU(needed int64) error {
// Assumes s.mu is already locked by the caller

for s.maxMemory.Load() > 0 && s.currentMemory.Load()+needed > s.maxMemory.Load() {
var oldestName string
var oldestTime time.Time
first := true

s.vectors.Range(func(name string, ds *Dataset) bool {
if first || ds.LastAccess().Before(oldestTime) {
oldestName = name
oldestTime = ds.LastAccess()
first = false
}
return true
})

if first {
// No datasets to evict
return NewResourceExhaustedError("memory", "limit exceeded and no datasets to evict")
}

// Evict oldest
if ds, ok := s.vectors.Get(oldestName); ok {
dsLockStart4 := time.Now()
ds.mu.Lock()
metrics.DatasetLockWaitDurationSeconds.WithLabelValues("snapshot").Observe(time.Since(dsLockStart4).Seconds())
for _, r := range ds.Records {
s.currentMemory.Add(-CachedRecordSize(r))
r.Release()
}
ds.Records = nil
ds.mu.Unlock()
s.vectors.Delete(oldestName)
s.logger.Info("Evicted dataset due to LRU", zap.Any("name", oldestName), zap.Any("freed", calculateDatasetSize(ds)))
metrics.EvictionsTotal.WithLabelValues("lru").Inc()
}
}
return nil
}

func calculateDatasetSize(ds *Dataset) int64 {
size := int64(0)
for _, r := range ds.Records {
size += CachedRecordSize(r)
}
return size
}

// ensureTimestamp ensures the record has a timestamp column, adding one if missing.
// Delegates to EnsureTimestampZeroCopy for optimized zero-copy implementation:
// - Pre-allocated timestamp builder with Reserve() for single allocation
// - Batch AppendValues instead of per-row Append (3-4x faster)
// - Proper Retain() for ref-counted zero-copy column references
func (s *VectorStore) ensureTimestamp(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
return EnsureTimestampZeroCopy(s.mem, rec)
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
if m.NumGC > 0 {
lastPauseNs := m.PauseNs[(m.NumGC+255)%256]
metrics.GcPauseDurationSeconds.Observe(float64(lastPauseNs) / 1e9)
	}
}

func (s *VectorStore) updateVectorMetrics(rec arrow.RecordBatch) {
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
// Trigger CI

// StartWALCheckTicker starts the background WAL size check loop
func (s *VectorStore) StartWALCheckTicker(interval time.Duration) {
ticker := time.NewTicker(interval)
go func() {
for range ticker.C {
s.checkWALSize()
}
}()
}

// checkWALSize checks if the WAL file size exceeds the limit and triggers a snapshot
func (s *VectorStore) checkWALSize() {
limit := s.maxWALSize.Load()
dataPath := s.dataPath

if limit <= 0 || dataPath == "" {
return
}

// Stat WAL file directly from filesystem
walPath := filepath.Join(dataPath, walFileName)
stat, err := os.Stat(walPath)
if err != nil {
if !os.IsNotExist(err) {
s.logger.Error("Failed to stat WAL file", zap.Error(err))
}
return
}

if stat.Size() > limit {
s.logger.Info("WAL size exceeded limit, triggering snapshot", zap.Any("current_size", stat.Size()), zap.Int64("limit", limit))
if err := s.Snapshot(); err != nil {
s.logger.Error("Failed to create snapshot triggered by WAL size", zap.Error(err))
}
}
}


func (s *VectorStore) startIndexingWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
	s.indexWg.Add(1)
		go func() {
		defer s.indexWg.Done()
			for job := range s.indexQueue.Jobs() {
	// Track index queue depth
metrics.IndexQueueDepth.Set(float64(s.indexQueue.Len()))
				ds, ok := s.vectors.Get(job.DatasetName)
				if !ok || ds.Index == nil {
					continue
				}
				if err := ds.Index.Add(job.BatchIdx, job.RowIdx); err != nil {
					s.logger.Error("Async index add failed", zap.Any("dataset", job.DatasetName), zap.Error(err))
				}
	// Record job latency
metrics.IndexJobLatencySeconds.WithLabelValues(job.DatasetName).Observe(time.Since(job.CreatedAt).Seconds())
			}
		}()
	}
}
