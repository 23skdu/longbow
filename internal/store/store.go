package store

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/mesh"
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
	maxWALSize    atomic.Int64  // Added for WAL
	sequence      atomic.Uint64 // Global operation sequence

	// Persistence
	dataPath      string
	wal           WAL         // For synchronous writes/snapshots
	walMu         sync.Mutex  // Protects wal
	walBatcher    *WALBatcher // For async batched writes
	snapshotReset chan time.Duration
	ttlDuration   time.Duration

	indexQueue *IndexJobQueue // Integrated HNSW

	indexWg sync.WaitGroup // For background workers
	mu      sync.RWMutex   // Protects datasets map (global lock, replaced by ShardedMap technically but kept for simple map access)

	// Mesh integration
	Mesh            *mesh.Gossip
	meshStatusCache *MeshStatusCache // Cache for mesh status serialization

	// NUMA integration (Phase 4/5)
	numaTopology *NUMATopology
}

func NewVectorStore(mem memory.Allocator, logger *zap.Logger, maxMemory, maxWALSize int64, ttl time.Duration) *VectorStore {
	cfg := DefaultIndexJobQueueConfig()

	// Detect NUMA topology (Phase 4/5)
	topo, err := DetectNUMATopology()
	if err != nil {
		logger.Warn("Failed to detect NUMA topology", zap.Error(err))
		topo = &NUMATopology{NumNodes: 1}
	}
	if topo.NumNodes > 1 {
		logger.Info("NUMA topology detected",
			zap.Int("nodes", topo.NumNodes),
			zap.String("topology", topo.String()))
	}

	s := &VectorStore{
		mem:             mem,
		logger:          logger,
		datasets:        make(map[string]*Dataset),
		indexQueue:      NewIndexJobQueue(cfg),
		meshStatusCache: NewMeshStatusCache(100 * time.Millisecond), // Cache mesh status for 100ms
		numaTopology:    topo,
	}

	// Start workers based on CPU count (Phase 5: Scale up)
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}
	s.startNUMAWorkers(numWorkers)

	return s
}
func (s *VectorStore) SetMesh(m *mesh.Gossip) {
	s.Mesh = m
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
	s.mu.RLock()
	datasets := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		datasets = append(datasets, ds)
	}
	s.mu.RUnlock()

	for _, ds := range datasets {
		info := &flight.FlightInfo{
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{ds.Name},
			},
		}
		if err := stream.Send(info); err != nil {
			return err
		}
	}
	return nil
}

func (s *VectorStore) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(desc.Path) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Empty path")
	}
	name := desc.Path[0]
	ds, err := s.getDataset(name)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     int64(len(ds.Records)),
		TotalBytes:       ds.SizeBytes.Load(),
	}, nil
}
func (s *VectorStore) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return nil, nil
}

// DoAction handles custom actions like deletion and status
func (s *VectorStore) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case "cluster-status":
		if s.Mesh == nil {
			return status.Error(codes.Unavailable, "gossip mesh not enabled")
		}
		members := s.Mesh.GetMembers()
		// Sort by ID for consistent output
		sort.Slice(members, func(i, j int) bool {
			return members[i].ID < members[j].ID
		})

		resp := map[string]interface{}{
			"self":    s.Mesh.GetIdentity(),
			"members": members,
			"count":   len(members),
		}

		body, err := json.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to serialize status: %v", err)
		}

		if err := stream.Send(&flight.Result{Body: body}); err != nil {
			return err
		}
		return nil

	case "delete-vector":
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
// DoPut - Optimized implementation with batching
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

	s.logger.Info("DoPut started (Batched)", zap.String("name", name))

	s.mu.Lock()
	if _, ok := s.datasets[name]; !ok {
		// Create new dataset with schema from reader
		s.datasets[name] = NewDataset(name, r.Schema())
	}
	ds = s.datasets[name]
	s.mu.Unlock()

	// Batching configuration
	const maxBatchSize = 100
	batch := make([]arrow.RecordBatch, 0, maxBatchSize)

	// Helper to flush batch
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Optimization: Concatenate small batches into one large batch
		// to reduce WAL overhead and lock contention.
		var combined arrow.RecordBatch
		if len(batch) == 1 {
			combined = batch[0]
			combined.Retain()
		} else {
			var err error
			combined, err = s.concatenateBatches(batch)
			if err != nil {
				s.logger.Error("Failed to concatenate batches", zap.Error(err))
				// Fallback to processing individually
				if err := s.flushPutBatch(ds, name, batch); err != nil {
					return err
				}
				batch = batch[:0]
				return nil
			}
		}
		defer combined.Release()

		// Flush single combined batch
		if err := s.flushPutBatch(ds, name, []arrow.RecordBatch{combined}); err != nil {
			return err
		}

		// Clear batch slice
		for _, b := range batch {
			b.Release()
		}
		batch = batch[:0]
		return nil
	}

	for r.Next() {
		rec := r.Record()
		rec.Retain()
		batch = append(batch, rec)

		if len(batch) >= maxBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if r.Err() != nil {
		s.logger.Error("DoPut stream error", zap.Error(r.Err()))
		// Cleanup pending
		for _, b := range batch {
			b.Release()
		}
		return r.Err()
	}

	// Flush remaining
	if len(batch) > 0 {
		if err := flush(); err != nil {
			for _, b := range batch {
				b.Release()
			}
			return err
		}
	}

	s.logger.Info("DoPut completed (Batched)", zap.String("name", name))
	return stream.Send(&flight.PutResult{})
}

// flushPutBatch handles writing a batch of records to WAL and memory
func (s *VectorStore) flushPutBatch(ds *Dataset, name string, batch []arrow.RecordBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// 1. Write to WAL (Iterate)
	ts := time.Now().UnixNano()
	if s.walBatcher != nil {
		for _, rec := range batch {
			seq := s.sequence.Add(1)
			if err := s.walBatcher.Write(rec, name, seq, ts); err != nil {
				s.logger.Error("Failed to write to WAL (batch)", zap.Error(err))
				metrics.WalWritesTotal.WithLabelValues("error").Inc()
				// Continue processing, but metrics
			} else {
				metrics.WalWritesTotal.WithLabelValues("ok").Inc()
			}
		}
	}

	// 2. Append to Memory AND Initialize Index if needed (Batch Lock)
	totalSize := int64(0)
	totalRows := int64(0)

	// Calculate size and prep metrics first (outside lock)
	for _, rec := range batch {
		batchSize := estimateBatchSize(rec)
		totalSize += batchSize
		totalRows += int64(rec.NumRows())

		metrics.DoPutPayloadSizeBytes.Observe(float64(batchSize))
		// Advise Memory: Random access for HNSW
		AdviseRecord(rec, AdviceRandom)
	}

	s.currentMemory.Add(totalSize)
	ds.SizeBytes.Add(totalSize)
	metrics.FlightRowsProcessed.WithLabelValues("put", "ok").Add(float64(totalRows))

	ds.dataMu.Lock()
	dsLockStart := time.Now()

	// Lazy Index Initialization (moved from DoPut)
	if ds.Index == nil {
		// Use AutoShardingIndex by default
		config := AutoShardingConfig{
			ShardThreshold: 10000,
			ShardCount:     runtime.NumCPU(),
		}
		ds.Index = NewAutoShardingIndex(ds, config)
	}

	batchStartIdx := len(ds.Records)
	ds.Records = append(ds.Records, batch...)
	ds.dataMu.Unlock()
	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	// 3. Update LWW/Merkle and Queue Indexing
	for i, rec := range batch {
		s.updateLWWAndMerkle(ds, rec, ts)

		batchIdx := batchStartIdx + i
		numRows := int(rec.NumRows())
		for r := 0; r < numRows; r++ {
			rec.Retain()
			if !s.indexQueue.Send(IndexJob{
				DatasetName: name,
				Record:      rec,
				BatchIdx:    batchIdx,
				RowIdx:      r,
				CreatedAt:   time.Now(),
			}) {
				rec.Release()
			}
		}
	}

	return nil
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

	// Parallel Processing with Pipeline Support (Phase 5)
	numWorkers := runtime.NumCPU()
	if numWorkers > len(ds.Records) {
		numWorkers = len(ds.Records)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	resultsChan := make(chan arrow.RecordBatch, numWorkers*2)
	// Buffer 1 to prevent blocking on first error check
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	// Determine execution strategy
	var stageChan <-chan PipelineStage
	usePipeline := ShouldUsePipeline(len(ds.Records))

	if usePipeline {
		// Use prefetching pipeline
		pipeline := NewDoGetPipeline(8) // config.PipelineDepth default
		// ProcessRecords handles feeding safely
		stageChan = pipeline.ProcessRecords(ctx, ds.Records, ds.Tombstones, query.Filters, nil)
		metrics.DoGetPipelineStepsTotal.WithLabelValues("pipeline").Add(float64(len(ds.Records)))
		s.logger.Debug("Using DoGetPipeline", zap.Int("workers", numWorkers))
	} else {
		// Simple feeder for small datasets
		metrics.DoGetPipelineStepsTotal.WithLabelValues("simple").Add(float64(len(ds.Records)))
		c := make(chan PipelineStage, len(ds.Records))
		stageChan = c
		go func() {
			defer close(c)
			for i, rec := range ds.Records {
				var ts *Bitset
				// Map access is safe under RLock
				if t, ok := ds.Tombstones[i]; ok {
					ts = t
				}
				select {
				case c <- PipelineStage{
					Record:    rec,
					BatchIdx:  i,
					Tombstone: ts,
				}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Start Workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for stage := range stageChan {
				rec := stage.Record
				deleted := stage.Tombstone

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
					// Use zero-copy with tombstone filtering (Phase 5)
					if deleted != nil && deleted.Count() > 0 {
						processed, err = ZeroCopyRecordBatch(s.mem, rec, deleted)
						metrics.DoGetZeroCopyTotal.WithLabelValues("zero_copy_mask").Inc()
					} else {
						// No tombstones - just retain (zero-copy!)
						rec.Retain()
						processed = rec
						metrics.DoGetZeroCopyTotal.WithLabelValues("zero_copy_retain").Inc()
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

	// Monitor to close results channel
	go func() {
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

func (s *VectorStore) startNUMAWorkers(numWorkers int) {
	if s.numaTopology == nil || s.numaTopology.NumNodes <= 1 {
		// Fallback to regular workers
		s.startIndexingWorkers(numWorkers)
		return
	}

	// Distribute workers across NUMA nodes
	workersPerNode := numWorkers / s.numaTopology.NumNodes
	if workersPerNode < 1 {
		workersPerNode = 1
	}

	// Adjust total to account for rounding
	totalStarted := 0

	for nodeID := 0; nodeID < s.numaTopology.NumNodes; nodeID++ {
		for i := 0; i < workersPerNode; i++ {
			s.indexWg.Add(1)
			totalStarted++
			go func(node int) {
				defer s.indexWg.Done()

				// Pin to NUMA node
				if err := PinToNUMANode(s.numaTopology, node); err != nil {
					s.logger.Warn("Failed to pin to NUMA node",
						zap.Int("node", node), zap.Error(err))
				}

				// Create NUMA-aware allocator (future use for allocations)
				allocator := NewNUMAAllocator(s.numaTopology, node)

				metrics.NumaWorkerDistribution.WithLabelValues(strconv.Itoa(node)).Inc()
				s.logger.Info("Started NUMA-aware worker", zap.Int("node", node))

				s.runIndexWorker(allocator)
			}(nodeID)
		}
	}
	s.logger.Info("Started NUMA indexing workers", zap.Int("count", totalStarted), zap.Int("nodes", s.numaTopology.NumNodes))
}

func (s *VectorStore) startIndexingWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		s.indexWg.Add(1)
		go func() {
			defer s.indexWg.Done()
			s.runIndexWorker(nil)
		}()
	}
	s.logger.Info("Started indexing workers", zap.Int("count", numWorkers))
}

func (s *VectorStore) runIndexWorker(allocator memory.Allocator) {
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

		// ds.Index access
		if ds.Index != nil {
			// Adaptive Sharding handled by AutoShardingIndex wrapper
			// Note: We currently don't use 'allocator' in AddByRecord, but pinning provides main benefit.
			if err := ds.Index.AddByRecord(job.Record, job.RowIdx, job.BatchIdx); err != nil {
				s.logger.Error("Async index add failed", zap.Any("dataset", job.DatasetName), zap.Error(err))
			}
		}

		// Release our reference to the record
		job.Record.Release()
		// Record job latency
		metrics.IndexJobLatencySeconds.WithLabelValues(job.DatasetName).Observe(time.Since(job.CreatedAt).Seconds())
	}
}

// MapInternalToUserIDs maps internal HNSW IDs to user-provided IDs
func (s *VectorStore) MapInternalToUserIDs(ds *Dataset, results []SearchResult) []SearchResult {
	start := time.Now()
	defer func() {
		metrics.IDResolutionDuration.Observe(time.Since(start).Seconds())
	}()

	s.mu.RLock()
	hnswIndex, ok := ds.Index.(*HNSWIndex)
	s.mu.RUnlock()

	if !ok || hnswIndex == nil {
		// Fallback for AutoShardingIndex
		s.mu.RLock()
		autoIndex, isAuto := ds.Index.(*AutoShardingIndex)
		s.mu.RUnlock()

		if isAuto {
			// Transparently access current index
			// Note: We need to access it via reflection or if we are in same package (we are!).
			// But 'current' field in AutoShardingIndex is lower case.
			// As we are in 'store' package, we CAN access it.
			autoIndex.mu.RLock()
			current := autoIndex.current
			autoIndex.mu.RUnlock()

			if h, ok := current.(*HNSWIndex); ok {
				hnswIndex = h
			} else {
				return results
			}
		} else {
			return results
		}
	}

	mappedResults := make([]SearchResult, 0, len(results))

	// We need to access dataset records. The HNSW index locations point to Batch/Row.
	// We'll use those to look up the ID from the "id" column of the record batch.
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	for _, res := range results {
		// 1. Get location (Batch, Row) from HNSW internal ID
		loc, found := hnswIndex.GetLocation(res.ID)
		if !found {
			// If not found in index (race condition?), skip or keep
			continue
		}

		// 2. Access RecordBatch
		if loc.BatchIdx >= len(ds.Records) {
			continue
		}
		rec := ds.Records[loc.BatchIdx]

		// 3. Find 'id' column
		// Optimization: could cache column index if schema is consistent
		idColIdx := -1
		for i, f := range rec.Schema().Fields() {
			if f.Name == "id" {
				idColIdx = i
				break
			}
		}

		if idColIdx == -1 {
			// No ID column, treat internal ID as valid
			mappedResults = append(mappedResults, res)
			continue
		}

		col := rec.Column(idColIdx)

		// 4. Extract User ID
		// ID column can be uint32 or uint64 (or others).
		// VectorID is uint32. If user ID is uint64 > 2^32, we have a truncation issue.
		// For now, cast to VectorID (uint32).
		var resolvedID VectorID

		switch c := col.(type) {
		case *array.Uint32:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx))
			} else {
				resolvedID = res.ID // Fallback
			}
		case *array.Uint64:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx)) // Truncate if needed
			} else {
				resolvedID = res.ID
			}
		default:
			// Unsupported ID type
			resolvedID = res.ID
		}

		mappedResults = append(mappedResults, SearchResult{
			ID:    resolvedID,
			Score: res.Score,
		})
	}

	return mappedResults
}

// GetDataset retrieves a dataset by name.
func (s *VectorStore) GetDataset(name string) (*Dataset, error) {
	return s.getDataset(name)
}

func (s *VectorStore) getDataset(name string) (*Dataset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.datasets[name]
	if !ok {
		return nil, fmt.Errorf("dataset %q not found", name)
	}
	return ds, nil
}

func (s *VectorStore) updateLWWAndMerkle(ds *Dataset, rec arrow.RecordBatch, ts int64) {
	idColIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "id" {
			idColIdx = i
			break
		}
	}

	if idColIdx >= 0 {
		column := rec.Column(idColIdx)
		if ids, ok := column.(*array.Uint32); ok {
			for i := 0; i < int(rec.NumRows()); i++ {
				vid := VectorID(ids.Value(i))
				if ds.LWW.Update(vid, ts) {
					if ds.Merkle != nil {
						ds.Merkle.Update(vid, ts)
					}
				}
			}
		}
	}
}

func (s *VectorStore) MerkleRoot(name string) [32]byte {
	ds, err := s.getDataset(name)
	if err != nil {
		return [32]byte{}
	}
	return ds.Merkle.RootHash()
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

// concatenateBatches merges multiple record batches into one
func (s *VectorStore) concatenateBatches(batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to concatenate")
	}
	schema := batches[0].Schema()
	numCols := int(schema.NumFields())
	columns := make([]arrow.Array, numCols)
	defer func() {
		// Clean up if we fail mid-way
		for _, col := range columns {
			if col != nil {
				col.Release()
			}
		}
	}()

	for i := 0; i < numCols; i++ {
		// Collect arrays for this column from all batches
		colArrays := make([]arrow.Array, len(batches))
		for j, batch := range batches {
			colArrays[j] = batch.Column(i)
		}

		// Use Arrow's array.Concatenate
		concatenated, err := array.Concatenate(colArrays, s.mem)
		if err != nil {
			return nil, fmt.Errorf("failed to concatenate column %d: %w", i, err)
		}
		columns[i] = concatenated
	}

	// Calculate total rows
	totalRows := int64(0)
	for _, b := range batches {
		totalRows += b.NumRows()
	}

	return array.NewRecordBatch(schema, columns, totalRows), nil
}
