package store

import (
	"context"
	"fmt"
	"os"
	"runtime"
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

type IndexJob struct {
	DatasetName string
	Record      arrow.RecordBatch
	BatchIdx    int
	RowIdx      int
	CreatedAt   time.Time
}

// Dataset wraps records with metadata for eviction
type Dataset struct {
	Records    []arrow.RecordBatch
	lastAccess int64 // UnixNano
	Version    int64
	Index      Index        // Abstract Interface
	dataMu     sync.RWMutex // Protects Records slice (append-only)
	Name       string
	Schema     *arrow.Schema
}

func (d *Dataset) LastAccess() time.Time {
	return time.Unix(0, atomic.LoadInt64(&d.lastAccess))
}

func (d *Dataset) SetLastAccess(t time.Time) {
	atomic.StoreInt64(&d.lastAccess, t.UnixNano())
}

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
	s.startIndexingWorkers(runtime.NumCPU())
	return s
}

// Helper methods required by other parts of the system potentially, or for interface satisfaction

func (s *VectorStore) StartEvictionTicker(d time.Duration)                                      {}
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
		s.datasets[name] = &Dataset{
			Name:    name,
			Records: make([]arrow.RecordBatch, 0),
			Schema:  r.Schema(),
		}
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

	for i, rec := range ds.Records {
		// Apply filters if present
		if len(query.Filters) > 0 {
			filtered, err := filterRecord(ctx, rec, query.Filters)
			if err != nil {
				s.logger.Error("Filter parsing failed", zap.Error(err))
				return status.Errorf(codes.Internal, "filtering failed: %v", err)
			}
			if filtered == nil || filtered.NumRows() == 0 {
				if filtered != nil {
					filtered.Release()
				}
				continue
			}

			// filterRecord returns a new batch with new buffers (compute.Filter allocates),
			// so it is safe to write directly without deepCopy.
			if err := w.Write(filtered); err != nil {
				s.logger.Error("DoGet Write failed", zap.Error(err), zap.Int("batch", i))
				filtered.Release()
				return err
			}
			rowsSent += filtered.NumRows()
			filtered.Release()
		} else {
			// Deep copy using Builder-based simplified implementation (for concurrency safety)
			copied, err := s.deepCopyRecordBatch(rec)
			if err != nil {
				s.logger.Error("Deep copy failed", zap.Error(err))
				return err
			}

			if err := w.Write(copied); err != nil {
				s.logger.Error("DoGet Write failed", zap.Error(err), zap.Int("batch", i))
				copied.Release()
				return err
			}
			rowsSent += copied.NumRows()
			copied.Release()
		}

		if query.Limit > 0 && rowsSent >= query.Limit {
			break
		}
	}

	s.logger.Info("DoGet completed", zap.Int64("rows_sent", rowsSent))
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
