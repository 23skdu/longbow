package store

import (
	"context"
	"encoding/json"
	"fmt"

	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// VectorStore implements flight.FlightServer with minimal logic
type VectorStore struct {
	flight.BaseFlightServer
	mem           memory.Allocator
	logger        *zap.Logger
	datasets      map[string]*Dataset
	maxMemory     atomic.Int64
	currentMemory atomic.Int64
	maxWALSize    atomic.Int64 // Added for WAL

	// Persistence
	dataPath      string
	walFile       *os.File    // For synchronous writes/snapshots
	walMu         sync.Mutex  // Protects walFile
	walBatcher    *WALBatcher // For async batched writes
	snapshotReset chan time.Duration
	ttlDuration   time.Duration

	indexQueue *IndexJobQueue // Integrated HNSW

	indexWg sync.WaitGroup // For background workers
	mu      sync.RWMutex   // Protects datasets map (global lock, replaced by ShardedMap technically but kept for simple map access)
}

func NewVectorStore(mem memory.Allocator, logger *zap.Logger, maxMemory, maxWALSize int64, ttl time.Duration) *VectorStore {
	cfg := DefaultIndexJobQueueConfig()
	s := &VectorStore{
		mem:        mem,
		logger:     logger,
		datasets:   make(map[string]*Dataset),
		indexQueue: NewIndexJobQueue(cfg),
	}
	s.startIndexingWorkers(1)
	return s
}

// Helper methods required by other parts of the system potentially, or for interface satisfaction

func (s *VectorStore) StartEvictionTicker(d time.Duration) {
	ticker := time.NewTicker(d)
	go func() {
		for range ticker.C {
			s.evictIfNeeded()
		}
	}()
}

func (s *VectorStore) evictIfNeeded() {
	curr := s.currentMemory.Load()
	limit := s.maxMemory.Load()
	if curr <= limit {
		return
	}

	// Snapshot datasets list safely
	s.mu.RLock()
	candidates := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		candidates = append(candidates, ds)
	}
	s.mu.RUnlock()

	// Sort by LastAccess (Ascending = Oldest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].LastAccess().Before(candidates[j].LastAccess())
	})

	// Evict until memory < limit
	for _, ds := range candidates {
		if s.currentMemory.Load() <= limit {
			break
		}
		s.evictDataset(ds)
	}
}

func (s *VectorStore) evictDataset(ds *Dataset) {
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	size := ds.SizeBytes.Load()
	if size == 0 {
		return // Already empty or not tracked
	}

	s.logger.Info("Evicting dataset", zap.String("name", ds.Name), zap.Int64("size_bytes", size))

	// Clear records
	for _, rec := range ds.Records {
		rec.Release()
	}
	ds.Records = nil
	ds.Tombstones = make(map[int]*Bitset)
	// Reset HNSW? HNSW struct handles its own memory usually, but it references Arrow records.
	// If HNSW copied vectors (non-zero-copy), we need to clear/close index.
	// TODO: Phase 3 Zero-Copy relies on Records. Since we released Records, HNSW is now invalid if it holds pointers.
	// For "Copy" HNSW (current), it holds its own vectors. So we must clear HNSW too to free memory.
	ds.Index = nil

	// Update tracking
	ds.SizeBytes.Store(0)
	s.currentMemory.Add(-size)
	metrics.EvictionsTotal.WithLabelValues("lru").Inc()
}
func (s *VectorStore) StartWALCheckTicker(d time.Duration)                                      {}
func (s *VectorStore) UpdateConfig(maxMemory, maxWALSize int64, snapshotInterval time.Duration) {}
func (s *VectorStore) StartMetricsTicker(d time.Duration)                                       {}
func (s *VectorStore) GetWALQueueDepth() (int, int)                                             { return 0, 0 }

func (s *VectorStore) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	return nil
}
func (s *VectorStore) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, nil
}
func (s *VectorStore) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return nil, nil
}

// DoAction handles custom actions like deletion
func (s *VectorStore) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if action.Type == "delete-vector" {
		var curr map[string]interface{}
		if err := json.Unmarshal(action.Body, &curr); err != nil {
			// Try Unmarshal as []struct? No, map is safer for now
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		dsName, ok := curr["dataset"].(string)
		if !ok {
			return status.Error(codes.InvalidArgument, "missing dataset name")
		}

		var vid uint32
		if v, ok := curr["vector_id"].(float64); ok {
			vid = uint32(v)
		} else {
			return status.Error(codes.InvalidArgument, "missing or invalid vector_id")
		}

		ds, err := s.getDataset(dsName)
		if err != nil {
			return err
		}

		// Resolve location
		var loc Location
		var found bool
		if hnsw, ok := ds.Index.(*HNSWIndex); ok {
			loc, found = hnsw.GetLocation(VectorID(vid))
		} else {
			return status.Error(codes.Unimplemented, "index does not support deletion")
		}

		if !found {
			return status.Error(codes.NotFound, "vector id not found")
		}

		// set tombstone
		ds.dataMu.Lock()
		if ds.Tombstones[loc.BatchIdx] == nil {
			ds.Tombstones[loc.BatchIdx] = NewBitset()
		}
		ts := ds.Tombstones[loc.BatchIdx]
		ds.dataMu.Unlock()

		ts.Set(loc.RowIdx)
		metrics.TombstonesTotal.WithLabelValues(dsName).Inc()

		if err := stream.Send(&flight.Result{Body: []byte("deleted")}); err != nil {
			return err
		}
		return nil
	}
	return status.Error(codes.Unimplemented, "unknown action type "+action.Type)
}

// DoPut - Minimal implementation
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
	r, err := flight.NewRecordReader(stream)
	if err != nil {
		s.logger.Error("DoPut failed to create reader", zap.Error(err))
		return err
	}
	defer r.Release()

	var name string
	var ds *Dataset

	// Check descriptor immediately (sent with Schema)
	fd := r.LatestFlightDescriptor()
	if fd != nil && len(fd.Path) > 0 {
		name = fd.Path[0]
	} else {
		// Fallback or error
		return fmt.Errorf("missing flight descriptor path")
	}

	s.logger.Info("DoPut started (Minimal)", zap.String("name", name))

	s.mu.Lock()
	if _, ok := s.datasets[name]; !ok {
		// Create new dataset with schema from reader
		s.datasets[name] = NewDataset(name, r.Schema())
	}
	ds = s.datasets[name]
	s.mu.Unlock()

	// Read first batch
	if !r.Next() {
		if r.Err() != nil {
			return r.Err()
		}
		// Valid empty stream (schema only) -> but we already created dataset
		return nil
	}

	rec := r.Record()

	// Write to WAL first for durability
	if s.walBatcher != nil {
		if err := s.walBatcher.Write(rec, name); err != nil {
			s.logger.Error("Failed to write to WAL", zap.Error(err))
			metrics.WalWritesTotal.WithLabelValues("error").Inc()
			return err
		}
	}

	// Use dataMu for append
	dsLockStart1 := time.Now()

	// Metric: Payload Size & Rows (First Batch)
	batchSize := estimateBatchSize(rec)
	metrics.DoPutPayloadSizeBytes.Observe(float64(batchSize))
	metrics.FlightRowsProcessed.WithLabelValues("put", "ok").Add(float64(rec.NumRows()))

	// Track memory
	s.currentMemory.Add(batchSize)
	ds.SizeBytes.Add(batchSize)

	ds.dataMu.Lock()
	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart1).Seconds())

	// Store first record
	rec.Retain()
	if ds.Index == nil {
		ds.Index = NewHNSWIndex(ds)
	}
	batchIdx := len(ds.Records)
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	// Queue Indexing
	numRows := int(rec.NumRows())
	for i := 0; i < numRows; i++ {
		rec.Retain()
		if !s.indexQueue.Send(IndexJob{
			DatasetName: name,
			Record:      rec,
			BatchIdx:    batchIdx,
			RowIdx:      i,
			CreatedAt:   time.Now(),
		}) {
			rec.Release()
		}
	}

	// Read remaining
	for r.Next() {
		rec := r.Record()
		rec.Retain()

		// Metric: Payload Size & Rows
		batchSize := estimateBatchSize(rec)
		metrics.DoPutPayloadSizeBytes.Observe(float64(batchSize))
		metrics.FlightRowsProcessed.WithLabelValues("put", "ok").Add(float64(rec.NumRows()))

		// Track memory
		s.currentMemory.Add(batchSize)
		ds.SizeBytes.Add(batchSize)

		ds.dataMu.Lock()
		batchIdx = len(ds.Records)
		ds.Records = append(ds.Records, rec)
		ds.dataMu.Unlock()

		// Queue Indexing
		numRows := int(rec.NumRows())
		for i := 0; i < numRows; i++ {
			rec.Retain()
			if !s.indexQueue.Send(IndexJob{
				DatasetName: name,
				Record:      rec,
				BatchIdx:    batchIdx,
				RowIdx:      i,
				CreatedAt:   time.Now(),
			}) {
				rec.Release()
			}
		}
	}

	if r.Err() != nil {
		s.logger.Error("DoPut stream error", zap.Error(r.Err()))
		return r.Err()
	}

	s.logger.Info("DoPut completed (Minimal)", zap.String("name", name))
	return stream.Send(&flight.PutResult{})
}

// DoGet - Minimal implementation
func (s *VectorStore) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// Parse ticket
	query, err := ParseTicketQuerySafe(tkt.Ticket)
	if err != nil {
		// Fallback: treat as plain string name if parse fails
		sStr := string(tkt.Ticket)
		if len(sStr) > 0 && sStr[0] != '{' {
			query.Name = sStr
		} else {
			s.logger.Error("Failed to parse ticket", zap.Error(err))
			return status.Error(codes.InvalidArgument, "invalid ticket format")
		}
	}

	name := query.Name
	s.logger.Info("DoGet called", zap.String("name", name), zap.Int("filters", len(query.Filters)))

	ds, err := s.getDataset(name)
	if err != nil {
		return err
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	if len(ds.Records) == 0 {
		s.logger.Warn("Dataset empty")
		return nil
	}

	// Use first record's schema
	schema := ds.Records[0].Schema()

	s.logger.Info("DoGet starting write", zap.Int("batches", len(ds.Records)))

	// Create Writer WITHOUT options first to be safe
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer w.Close()

	ctx := stream.Context()
	rowsSent := int64(0)

	// Parallel Processing
	numWorkers := runtime.NumCPU()
	if numWorkers > len(ds.Records) {
		numWorkers = len(ds.Records)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	workChan := make(chan int, len(ds.Records))
	resultsChan := make(chan arrow.RecordBatch, numWorkers*2)
	errChan := make(chan error, 1) // Buffer 1 to prevent blocking on first error check
	var wg sync.WaitGroup

	// Start Workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range workChan {
				rec := ds.Records[i]

				// Prepare Tombstone
				var deleted *Bitset
				// Map access under RLock (already held by DoGet)
				// Accessing map is safe if we hold RLock and writers hold Lock.
				if ts, ok := ds.Tombstones[i]; ok {
					deleted = ts
				}

				var processed arrow.RecordBatch
				var err error

				if len(query.Filters) > 0 {
					filtered, err := filterRecord(ctx, rec, query.Filters)
					if err != nil {
						select {
						case errChan <- err:
						default:
						} // Try send error
						return
					}
					if filtered != nil && filtered.NumRows() > 0 {
						processed = filtered
					} else {
						if filtered != nil {
							filtered.Release()
						}
						continue
					}
				} else {
					if deleted != nil {
						processed, err = s.deepCopyRecordBatchWithMask(rec, deleted)
					} else {
						processed, err = s.deepCopyRecordBatch(rec)
					}
					if err != nil {
						select {
						case errChan <- err:
						default:
						}
						return
					}
				}

				// Send to results
				select {
				case resultsChan <- processed:
				case <-ctx.Done():
					processed.Release()
					return
				}
			}
		}()
	}

	// Feed Work
	go func() {
		for i := 0; i < len(ds.Records); i++ {
			workChan <- i
		}
		close(workChan)
		wg.Wait()
		close(resultsChan)
		close(errChan)
	}()

	// Consume Results (Sequential Write)
	for {
		select {
		case batch, ok := <-resultsChan:
			if !ok {
				resultsChan = nil // Channel closed
			} else {
				if err := w.Write(batch); err != nil {
					s.logger.Error("DoGet Write failed", zap.Error(err))
					batch.Release()
					return err
				}
				rowsSent += batch.NumRows()
				batch.Release()

				if query.Limit > 0 && rowsSent >= query.Limit {
					// Stop workers? Context cancel?
					// Ideally we cancel context for workers logic but here we just break read loop.
					// Workers might still produce some.
					// Since we don't have cancelable context for workers explicitly here (using stream.Context),
					// we can't easily stop them. They will finish or block on closed resultsChan?
					// No, we read until closed. If we break early, we must drain or assume cleanup.
					// For MVP: Break loop. Workers finish.
					// But if we break, `go func` feeding `workChan` might block on `resultsChan` sends if buffer full?
					// Yes.
					// FIX: Drain channel if breaking early.
					goto DRAIN
				}
			}
		case err, ok := <-errChan:
			if ok && err != nil {
				return err
			}
		}
		if resultsChan == nil {
			break
		}
	}

	// Normal exit
	return nil

DRAIN:
	// Drain remaining results to prevent worker deadlock
	go func() {
		for range resultsChan {
			// discard
		}
	}()

	s.logger.Info("DoGet completed", zap.Int64("rows_sent", rowsSent))
	metrics.FlightRowsProcessed.WithLabelValues("get", "ok").Add(float64(rowsSent))
	return nil
}

// deepCopyRecordBatch creates a full copy using Builders for safety
func (s *VectorStore) deepCopyRecordBatch(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if rec.NumRows() == 0 {
		rec.Retain()
		return rec, nil
	}

	cols := make([]arrow.Array, rec.NumCols())
	for i, col := range rec.Columns() {
		copied, err := s.copyArray(col)
		if err != nil {
			// Cleanup created columns
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, fmt.Errorf("failed to copy column %d: %w", i, err)
		}
		cols[i] = copied
	}
	return array.NewRecordBatch(rec.Schema(), cols, rec.NumRows()), nil
}

func (s *VectorStore) copyArray(arr arrow.Array) (arrow.Array, error) {
	if arr.Len() == 0 {
		arr.Retain()
		return arr, nil
	}

	switch arr.DataType().ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(s.mem)
		defer b.Release()
		input := arr.(*array.Int64)
		b.Reserve(input.Len())
		for i := 0; i < input.Len(); i++ {
			if input.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(input.Value(i))
			}
		}
		return b.NewArray(), nil

	case arrow.FLOAT32:
		b := array.NewFloat32Builder(s.mem)
		defer b.Release()
		input := arr.(*array.Float32)
		b.Reserve(input.Len())
		for i := 0; i < input.Len(); i++ {
			if input.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(input.Value(i))
			}
		}
		return b.NewArray(), nil

	case arrow.FLOAT64:
		b := array.NewFloat64Builder(s.mem)
		defer b.Release()
		input := arr.(*array.Float64)
		b.Reserve(input.Len())
		for i := 0; i < input.Len(); i++ {
			if input.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(input.Value(i))
			}
		}
		return b.NewArray(), nil

	case arrow.STRING:
		b := array.NewStringBuilder(s.mem)
		defer b.Release()
		input := arr.(*array.String)
		b.Reserve(input.Len())
		for i := 0; i < input.Len(); i++ {
			if input.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(input.Value(i))
			}
		}
		return b.NewArray(), nil

	case arrow.TIMESTAMP:
		b := array.NewTimestampBuilder(s.mem, arr.DataType().(*arrow.TimestampType))
		defer b.Release()
		input := arr.(*array.Timestamp)
		b.Reserve(input.Len())
		for i := 0; i < input.Len(); i++ {
			if input.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(input.Value(i))
			}
		}
		return b.NewArray(), nil

	case arrow.FIXED_SIZE_LIST:
		input := arr.(*array.FixedSizeList)
		listSize := int(input.DataType().(*arrow.FixedSizeListType).Len())

		// Create builder for the values (e.g. Float32Builder)
		valueType := input.DataType().(*arrow.FixedSizeListType).Elem()

		if valueType.ID() == arrow.FLOAT32 {
			b := array.NewFixedSizeListBuilder(s.mem, int32(listSize), arrow.PrimitiveTypes.Float32)
			defer b.Release()
			valBuilder := b.ValueBuilder().(*array.Float32Builder)

			b.Reserve(input.Len())
			values := input.ListValues().(*array.Float32) // Assuming values are contiguous logic

			// Iterate lists
			for i := 0; i < input.Len(); i++ {
				if input.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(true)
					// Append 'listSize' values
					start := i * listSize
					for k := 0; k < listSize; k++ {
						val := values.Value(start + k)
						valBuilder.Append(val)
					}
				}
			}
			return b.NewArray(), nil
		}

		// Fallback for other list types: just Retain (shallow copy) to be safe
		arr.Retain()
		return arr, nil

	default:
		// Fallback for types we don't explicitly handle
		arr.Retain()
		return arr, nil
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

				s.mu.RLock()
				ds, ok := s.datasets[job.DatasetName]
				s.mu.RUnlock()

				if !ok {
					job.Record.Release()
					continue
				}

				// ds.Index access requires lock? HNSW is thread safe usually
				if ds.Index != nil {
					if err := ds.Index.AddByRecord(job.Record, job.RowIdx, job.BatchIdx); err != nil {
						s.logger.Error("Async index add failed", zap.Any("dataset", job.DatasetName), zap.Error(err))
					}
				}
				// Release our reference to the record
				job.Record.Release()
				// Record job latency
				metrics.IndexJobLatencySeconds.WithLabelValues(job.DatasetName).Observe(time.Since(job.CreatedAt).Seconds())
			}
		}()
	}
}

// getDataset retrieves a dataset by name.
func (s *VectorStore) getDataset(name string) (*Dataset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.datasets[name]
	if !ok {
		return nil, fmt.Errorf("dataset %q not found", name)
	}
	return ds, nil
}

// estimateBatchSize calculates appropriate size in bytes of a record batch
func estimateBatchSize(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}
	size := int64(0)
	for _, col := range rec.Columns() {
		// Approximate: sum of all buffer lengths
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
		// Recurse for children (e.g. List arrays)
		// Note: Children() returns []ArrowData, which is internal.
		// For correctness with Arrow Go, we might rely on Buffers() mostly.
		// Detailed recursion is complex without `array.Data` access if not exported.
		// However col.Data() gives ArrayData which has Children().
		for _, child := range col.Data().Children() {
			for _, buf := range child.Buffers() {
				if buf != nil {
					size += int64(buf.Len())
				}
			}
		}
	}
	return size
}

// deepCopyRecordBatchWithMask creates a copy excluding rows marked in the bitset
func (s *VectorStore) deepCopyRecordBatchWithMask(rec arrow.RecordBatch, mask *Bitset) (arrow.RecordBatch, error) {
	if rec.NumRows() == 0 {
		rec.Retain()
		return rec, nil
	}

	// Calculate output size
	inRows := int(rec.NumRows())
	outRows := 0
	for i := 0; i < inRows; i++ {
		if !mask.Contains(i) {
			outRows++
		}
	}

	if outRows == 0 {
		// Return empty batch
		cols := make([]arrow.Array, rec.NumCols())
		for i, col := range rec.Columns() {
			// Create empty array of same type
			col.Retain()
			cols[i] = array.NewSlice(col, 0, 0)
		}
		return array.NewRecordBatch(rec.Schema(), cols, 0), nil
	}

	cols := make([]arrow.Array, rec.NumCols())
	for i, col := range rec.Columns() {
		copied, err := s.copyArrayWithMask(col, mask, outRows)
		if err != nil {
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, fmt.Errorf("failed to copy column %d: %w", i, err)
		}
		cols[i] = copied
	}
	return array.NewRecordBatch(rec.Schema(), cols, int64(outRows)), nil
}

func (s *VectorStore) copyArrayWithMask(arr arrow.Array, mask *Bitset, outCount int) (arrow.Array, error) {
	if arr.Len() == 0 {
		arr.Retain()
		return arr, nil
	}

	switch arr.DataType().ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(s.mem)
		defer b.Release()
		b.Reserve(outCount)
		input := arr.(*array.Int64)
		for i := 0; i < input.Len(); i++ {
			if !mask.Contains(i) {
				if input.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(input.Value(i))
				}
			}
		}
		return b.NewArray(), nil

	case arrow.FLOAT32:
		b := array.NewFloat32Builder(s.mem)
		defer b.Release()
		b.Reserve(outCount)
		input := arr.(*array.Float32)
		for i := 0; i < input.Len(); i++ {
			if !mask.Contains(i) {
				if input.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(input.Value(i))
				}
			}
		}
		return b.NewArray(), nil

	case arrow.FLOAT64:
		b := array.NewFloat64Builder(s.mem)
		defer b.Release()
		b.Reserve(outCount)
		input := arr.(*array.Float64)
		for i := 0; i < input.Len(); i++ {
			if !mask.Contains(i) {
				if input.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(input.Value(i))
				}
			}
		}
		return b.NewArray(), nil

	case arrow.STRING:
		b := array.NewStringBuilder(s.mem)
		defer b.Release()
		b.Reserve(outCount)
		input := arr.(*array.String)
		for i := 0; i < input.Len(); i++ {
			if !mask.Contains(i) {
				if input.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(input.Value(i))
				}
			}
		}
		return b.NewArray(), nil

	case arrow.TIMESTAMP:
		b := array.NewTimestampBuilder(s.mem, arr.DataType().(*arrow.TimestampType))
		defer b.Release()
		b.Reserve(outCount)
		input := arr.(*array.Timestamp)
		for i := 0; i < input.Len(); i++ {
			if !mask.Contains(i) {
				if input.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(input.Value(i))
				}
			}
		}
		return b.NewArray(), nil

	case arrow.FIXED_SIZE_LIST:
		input := arr.(*array.FixedSizeList)
		listSize := int(input.DataType().(*arrow.FixedSizeListType).Len())
		valueType := input.DataType().(*arrow.FixedSizeListType).Elem()

		if valueType.ID() == arrow.FLOAT32 {
			b := array.NewFixedSizeListBuilder(s.mem, int32(listSize), arrow.PrimitiveTypes.Float32)
			defer b.Release()
			valBuilder := b.ValueBuilder().(*array.Float32Builder)

			b.Reserve(outCount)
			values := input.ListValues().(*array.Float32)

			for i := 0; i < input.Len(); i++ {
				if !mask.Contains(i) {
					if input.IsNull(i) {
						b.AppendNull()
					} else {
						b.Append(true)
						start := i * listSize
						for k := 0; k < listSize; k++ {
							val := values.Value(start + k)
							valBuilder.Append(val)
						}
					}
				}
			}
			return b.NewArray(), nil
		}

		// Fallback: Copy all
		arr.Retain()
		return arr, nil

	default:
		// Fallback
		arr.Retain()
		return arr, nil
	}
}
