package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
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
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// VectorStore implements flight.FlightServer with minimal logic
type VectorStore struct {
	flight.BaseFlightServer
	mem           memory.Allocator
	logger        *zap.Logger
	datasets      map[string]*Dataset
	maxMemory     atomic.Int64
	currentMemory atomic.Int64
	memoryConfig  MemoryConfig

	sequence atomic.Uint64 // Global operation sequence

	// Persistence
	dataPath      string
	wal           WAL         // For synchronous writes/snapshots
	walMu         sync.Mutex  // Protects wal
	walBatcher    *WALBatcher // For async batched writes
	snapshotReset chan time.Duration

	indexQueue *IndexJobQueue // Integrated HNSW

	// Lifecycle
	stopChan chan struct{}
	stopOnce sync.Once
	indexWg  sync.WaitGroup // For background workers
	mu       sync.RWMutex   // Protects datasets map (global lock, replaced by ShardedMap technically but kept for simple map access)

	// Mesh integration
	Mesh            *mesh.Gossip
	meshStatusCache *MeshStatusCache // Cache for mesh status serialization

	// NUMA integration (Phase 4/5)
	numaTopology *NUMATopology

	// Hybrid search (Phase 20)
	hybridSearchConfig HybridSearchConfig
	bm25Index          *BM25InvertedIndex

	// DoGet pipeline subsystem
	doGetPipelinePool *DoGetPipelinePool
	pipelineThreshold int

	// Column-based inverted index for O(1) equality filter lookups
	columnIndex    *ColumnInvertedIndex
	indexedColumns []string // columns to index for fast equality lookups

	// Compaction (Phase 11/14)
	compactionConfig CompactionConfig
	compactionWorker *CompactionWorker

	// Auto-sharding (Phase 13)
	autoShardingConfig AutoShardingConfig

	// Namespace management
	nsManager *namespaceManager

	// GPU acceleration (optional)

	// Shutdown and lifecycle (Phase 6/21)
	shutdownState int32
	walFile       *os.File
	workerWg      sync.WaitGroup
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

	if logger == nil {
		logger = zap.NewNop()
	}

	s := &VectorStore{
		mem:                mem,
		logger:             logger,
		datasets:           make(map[string]*Dataset),
		indexQueue:         NewIndexJobQueue(cfg),
		meshStatusCache:    NewMeshStatusCache(100 * time.Millisecond), // Cache mesh status for 100ms
		numaTopology:       topo,
		hybridSearchConfig: DefaultHybridSearchConfig(),
		columnIndex:        NewColumnInvertedIndex(),
		compactionConfig:   DefaultCompactionConfig(),
		autoShardingConfig: DefaultAutoShardingConfig(),
		nsManager:          newNamespaceManager(),
		stopChan:           make(chan struct{}),
		snapshotReset:      make(chan time.Duration, 1),
		memoryConfig:       DefaultMemoryConfig(),
	}
	s.maxMemory.Store(maxMemory)
	s.memoryConfig.MaxMemory = maxMemory
	// Initialize global BM25 if needed (default disabled)
	if s.hybridSearchConfig.Enabled {
		s.bm25Index = NewBM25InvertedIndex(s.hybridSearchConfig.BM25)
	}

	// Initialize compaction if enabled (Phase 11/14)
	if s.compactionConfig.Enabled {
		s.compactionWorker = NewCompactionWorker(s, s.compactionConfig)
		s.compactionWorker.Start()
	}

	// Start workers based on CPU count (Phase 5: Scale up)
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}
	s.startNUMAWorkers(numWorkers)

	// Start memory metrics updater
	s.startMemoryMetricsUpdater()

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
	// 1. Mark as evicting BEFORE acquiring lock
	// This allows in-flight queries to detect eviction and fail gracefully
	if !ds.evicting.CompareAndSwap(false, true) {
		return // Already evicting
	}

	// 2. Acquire WRITE lock (blocks all readers)
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	size := ds.SizeBytes.Load()
	if size == 0 {
		ds.evicting.Store(false) // Reset flag
		return                   // Already empty or not tracked
	}

	s.logger.Warn("EVICTION TRIGGERED",
		zap.String("dataset", ds.Name),
		zap.Int64("size_bytes", size),
		zap.Stack("stack"))

	// 3. Clear records (safe now - no readers due to write lock)
	for _, rec := range ds.Records {
		rec.Release()
	}
	ds.Records = nil
	ds.Tombstones = make(map[int]*Bitset)
	ds.Index = nil

	// 4. Update tracking
	ds.SizeBytes.Store(0)
	s.currentMemory.Add(-size)
	metrics.EvictionsTotal.WithLabelValues("lru").Inc()

	// Note: evicting flag stays true to prevent future queries
}

// IterateDatasets safely iterates over all datasets for background tasks.
func (s *VectorStore) IterateDatasets(fn func(ds *Dataset)) {
	s.mu.RLock()
	// Copy to slice to avoid holding lock during callback
	datasets := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		datasets = append(datasets, ds)
	}
	s.mu.RUnlock()

	for _, ds := range datasets {
		fn(ds)
	}
}

// SetIndexedColumns updates columns that should be indexed for fast equality lookups
func (s *VectorStore) SetIndexedColumns(cols []string) {
	s.indexedColumns = cols
}

// GetIndexedColumns returns columns currently being indexed
func (s *VectorStore) GetIndexedColumns() []string {
	return s.indexedColumns
}

// IndexRecordColumns indexes specific columns for fast equality lookups
func (s *VectorStore) IndexRecordColumns(datasetName string, rec arrow.RecordBatch, batchIdx int) {
	if s.columnIndex == nil || len(s.indexedColumns) == 0 {
		return
	}
	s.columnIndex.IndexRecord(datasetName, batchIdx, rec, s.indexedColumns)
}

// SetAutoShardingConfig updates the auto-sharding configuration
func (s *VectorStore) SetAutoShardingConfig(cfg AutoShardingConfig) {
	s.autoShardingConfig = cfg
}

// GetAutoShardingConfig returns the current auto-sharding configuration
func (s *VectorStore) GetAutoShardingConfig() AutoShardingConfig {
	return s.autoShardingConfig
}

// extractVectorFromCol extracts a float32 vector from an arrow.Array at rowIdx
func extractVectorFromCol(arr arrow.Array, rowIdx int) []float32 {
	if arr == nil {
		return nil
	}
	listArr, ok := arr.(*array.FixedSizeList)
	if !ok {
		return nil
	}
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width
	end := start + width
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()
	if start < 0 || end > floatArr.Len() {
		return nil
	}
	return floatArr.Float32Values()[start:end]
}

func (s *VectorStore) checkAndMigrateToSharded(ds *Dataset) error {
	// Placeholder logic: check if dataset size exceeds threshold and migrate index to sharded
	if !s.autoShardingConfig.Enabled {
		return nil
	}
	// Migration logic would go here
	return nil
}

// WarmupStats holds statistics about the warmup operation
type WarmupStats struct {
	DatasetsWarmed   int
	DatasetsSkipped  int
	TotalNodesWarmed int
	Duration         time.Duration
}

func (w WarmupStats) String() string {
	return fmt.Sprintf("Warmed %d datasets (%d skipped), touched %d nodes in %v",
		w.DatasetsWarmed, w.DatasetsSkipped, w.TotalNodesWarmed, w.Duration)
}

// Warmup iterates through all datasets and warms up their indexes
func (s *VectorStore) Warmup() WarmupStats {
	start := time.Now()
	stats := WarmupStats{}

	s.mu.RLock()
	datasets := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		datasets = append(datasets, ds)
	}
	s.mu.RUnlock()

	for _, ds := range datasets {
		ds.dataMu.RLock()
		idx := ds.Index
		ds.dataMu.RUnlock()

		if idx != nil {
			nodes := idx.Warmup()
			stats.TotalNodesWarmed += nodes
			stats.DatasetsWarmed++
		} else {
			stats.DatasetsSkipped++
		}
	}

	stats.Duration = time.Since(start)
	return stats
}

func (s *VectorStore) StartWALCheckTicker(d time.Duration)                                      {}
func (s *VectorStore) UpdateConfig(maxMemory, maxWALSize int64, snapshotInterval time.Duration) {}
func (s *VectorStore) StartMetricsTicker(d time.Duration)                                       {}
func (s *VectorStore) GetWALQueueDepth() (int, int)                                             { return 0, 0 }

func (s *VectorStore) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	var query TicketQuery
	var err error
	if c != nil && len(c.Expression) > 0 {
		query, err = ParseTicketQuerySafe(c.Expression)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Invalid criteria: %v", err)
		}
	}

	s.mu.RLock()
	datasets := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		datasets = append(datasets, ds)
	}
	s.mu.RUnlock()

	for _, ds := range datasets {
		// Apply filters
		match := true
		for _, f := range query.Filters {
			switch f.Field {
			case "name":
				if f.Operator == "contains" {
					if !strings.Contains(ds.Name, f.Value) {
						match = false
					}
				}
			case "rows":
				var numRows int64
				ds.dataMu.RLock()
				for _, rec := range ds.Records {
					numRows += rec.NumRows()
				}
				ds.dataMu.RUnlock()

				val, err := strconv.ParseInt(f.Value, 10, 64)
				if err != nil {
					match = false
					break
				}
				switch f.Operator {
				case ">":
					if !(numRows > val) {
						match = false
					}
				case "<=":
					if !(numRows <= val) {
						match = false
					}
				case "==":
					if !(numRows == val) {
						match = false
					}
				}
			}
			if !match {
				break
			}
		}

		if match {
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

		// Resolve location using interface method (works for all index types)
		loc, found := ds.Index.GetLocation(VectorID(vid))
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
		ds := NewDataset(name, r.Schema())
		ds.Topo = s.numaTopology
		s.datasets[name] = ds
	}
	ds = s.datasets[name]

	// Initialize GPU if enabled
	if hnswIdx, ok := ds.Index.(*HNSWIndex); ok {
		s.initGPUIfEnabled(hnswIdx)
	}
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
		// defer combined.Release() // This was commented out, no change needed.

		// Flush single combined batch
		if err := s.flushPutBatch(ds, name, []arrow.RecordBatch{combined}); err != nil {
			combined.Release()
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
		rec := r.RecordBatch()

		// Adaptive Batching (Option 1):
		// If the record is large enough and we don't have pending small records,
		// write it directly to avoid concatenation/slice overhead.
		if len(batch) == 0 && rec.NumRows() >= 100 {
			rec.Retain()
			if err := s.flushPutBatch(ds, name, []arrow.RecordBatch{rec}); err != nil {
				rec.Release()
				return err
			}
			// Transfer ownership of our Retain() to ds.Records via flushPutBatch
			// Wait, previous code:
			// rec.Retain()
			// batch = append(batch, rec)
			// flush() -> flushPutBatch(batch) -> inside flushPutBatch?
			// It appends to ds.Records. ds.Records owns it?
			// In flushPutBatch: "ds.Records = append(ds.Records, batch...)"
			// Yes, it takes ownership.
			// DOES flushPutBatch Retain?
			// Let's check flushPutBatch in store.go.
			continue
		}

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
	return nil
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

	// Check memory limit before accepting write
	if err := s.checkMemoryBeforeWrite(totalSize); err != nil {
		return err
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

	// Record NUMA node for these batches
	currCPU := GetCurrentCPU()
	currNode := -1
	if s.numaTopology != nil {
		currNode = s.numaTopology.GetNodeForCPU(currCPU)
	}
	for range batch {
		ds.BatchNodes = append(ds.BatchNodes, currNode)
	}

	ds.dataMu.Unlock()
	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	// 3. Update LWW/Merkle and Queue Indexing
	for i, rec := range batch {
		s.updateLWWAndMerkle(ds, rec, ts)

		batchIdx := batchStartIdx + i
		numRows := int(rec.NumRows())
		for r := 0; r < numRows; r++ {
			rec.Retain()
			// Retry loop for backpressure instead of dropping
			job := IndexJob{
				DatasetName: name,
				Record:      rec,
				BatchIdx:    batchIdx,
				RowIdx:      r,
				CreatedAt:   time.Now(),
			}

			// Try to send, backing off if full
			sent := false
			for attempt := 0; attempt < 100; attempt++ {
				if s.indexQueue.Send(job) {
					sent = true
					break
				}
				// Queue full - wait a bit to apply backpressure
				time.Sleep(5 * time.Millisecond)
			}

			if !sent {
				s.logger.Warn("Dropped index job after retries (queue full)", zap.String("dataset", name))
				rec.Release()
				metrics.IndexJobsDroppedTotal.Inc()
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
	// Determine execution strategy
	var stageChan <-chan PipelineStage
	usePipeline := s.shouldUsePipeline(len(ds.Records))
	var pipeline *DoGetPipeline

	if usePipeline {
		// Use prefetching pipeline
		if s.doGetPipelinePool != nil {
			pipeline = s.doGetPipelinePool.Get()
		} else {
			pipeline = NewDoGetPipeline(8, 16) // Fallback defaults
		}

		// ProcessRecords handles feeding safely
		stageChan = pipeline.ProcessRecords(ctx, ds.Records, ds.Tombstones, query.Filters, nil)
		metrics.DoGetPipelineStepsTotal.WithLabelValues("scan", "pipeline").Add(float64(len(ds.Records)))
		s.logger.Debug("Using DoGetPipeline", zap.Int("workers", pipeline.NumWorkers()))
	} else {
		// Simple feeder for small datasets
		metrics.DoGetPipelineStepsTotal.WithLabelValues("scan", "simple").Add(float64(len(ds.Records)))
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
					filterStart := time.Now()
					filtered, err := filterRecord(ctx, rec, query.Filters)
					metrics.FilterExecutionDurationSeconds.WithLabelValues(name).Observe(time.Since(filterStart).Seconds())
					if err != nil {
						select {
						case errChan <- err:
						default:
						} // Try send error
						return
					}
					if rec.NumRows() > 0 && filtered != nil {
						ratio := float64(filtered.NumRows()) / float64(rec.NumRows())
						metrics.FilterSelectivityRatio.WithLabelValues(name).Observe(ratio)
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
				// Verify Columns
				for i := 0; i < int(batch.NumCols()); i++ {
					col := batch.Column(i)
					if col.Len() != int(batch.NumRows()) {
						s.logger.Error("DoGet Batch Column Length Mismatch", zap.Int("col_idx", i), zap.Int("col_len", col.Len()), zap.Int64("batch_rows", batch.NumRows()))
					}
					// Check data length for specific types if known, or just ensure not nil
					if col.Data() == nil {
						s.logger.Error("DoGet Batch Column Data is NIL", zap.Int("col_idx", i))
					}
				}

				if err := w.Write(batch); err != nil {
					s.logger.Error("DoGet Write failed", zap.Error(err))
					batch.Release()
					return err
				}
				rowsSent += batch.NumRows()
				batch.Release()

				// Track stats for test verification
				if usePipeline {
					s.incrementPipelineBatches(1)
				}

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

	if pipeline != nil && s.doGetPipelinePool != nil {
		s.doGetPipelinePool.Put(pipeline)
	}

	// Normal exit
	return nil

DRAIN:
	if pipeline != nil && s.doGetPipelinePool != nil {
		s.doGetPipelinePool.Put(pipeline)
	}
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
			if job.Record != nil {
				job.Record.Release()
			}
			continue
		}

		// ds.Index access
		// ds.Index access
		var docID uint32
		if ds.Index != nil {
			// Adaptive Sharding handled by AutoShardingIndex wrapper
			// Note: We currently don't use 'allocator' in AddByRecord, but pinning provides main benefit.
			var err error
			docID, err = ds.Index.AddByRecord(job.Record, job.RowIdx, job.BatchIdx)
			if err != nil {
				s.logger.Error("Async index add failed", zap.Any("dataset", job.DatasetName), zap.Error(err))
			}
		}

		// Update Inverted Indexes (Hybrid Search)
		// We iterate over all columns, and if we find a String column, we index it.
		// For MVP, we index *all* string columns.
		// NOTE: This adds overhead. In production, we should check schema metadata.
		go func(job *IndexJob, ds *Dataset) { // Use new goroutine or same? Same is better for CPU usage control.
			// Actually, let's do it inline to respect numWorkers.
			// But we need to define "store" package? We are in "store" package.
			// job is IndexJob.
		}(nil, nil)

		schema := job.Record.Schema()
		for i, field := range schema.Fields() {
			if field.Type.ID() == arrow.STRING {
				// Get or Create Inverted Index for this column
				ds.dataMu.Lock()
				if ds.InvertedIndexes == nil {
					ds.InvertedIndexes = make(map[string]*InvertedIndex)
				}
				idx, ok := ds.InvertedIndexes[field.Name]
				if !ok {
					idx = NewInvertedIndex()
					ds.InvertedIndexes[field.Name] = idx
				}
				ds.dataMu.Unlock()

				colI := job.Record.Column(i)
				if col, ok := colI.(*array.String); ok {
					if job.RowIdx < col.Len() && col.IsValid(job.RowIdx) {
						text := col.Value(job.RowIdx)
						idx.Add(text, docID)

						// Also update BM25 index for all string columns (Phase 20)
						ds.dataMu.Lock()
						if ds.BM25Index == nil {
							// Lazy init BM25 with default config
							ds.BM25Index = NewBM25InvertedIndex(DefaultBM25Config())
						}
						bm25 := ds.BM25Index
						ds.dataMu.Unlock()

						if bm25 != nil {
							bm25.Add(VectorID(docID), text)
							metrics.BM25DocumentsIndexedTotal.Inc()
						}
					}
				} else {
					s.logger.Warn("Column type mismatch in runIndexWorker", zap.String("field", field.Name), zap.Any("type", colI.DataType()))
				}
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

	if ok && hnswIndex != nil {
		// Optimization: if it's a plain HNSW index, use its built-in mapping which is faster (Phase 14)
		// but wait, its built-in mapping might be what we are implementing here!
		// Let's stick to the store-side mapping for now as it's more flexible with Arrow types.
	} else {
		// Fallback for AutoShardingIndex
		s.mu.RLock()
		autoIndex, isAuto := ds.Index.(*AutoShardingIndex)
		s.mu.RUnlock()

		if isAuto {
			autoIndex.mu.RLock()
			current := autoIndex.current
			autoIndex.mu.RUnlock()

			if h, ok := current.(*HNSWIndex); ok {
				hnswIndex = h
			}
		}
	}

	if hnswIndex == nil {
		return results
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
		case *array.Int64:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx))
			} else {
				resolvedID = res.ID
			}
		case *array.Int32:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx))
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
		return nil, NewNotFoundError("dataset", name)
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

// filterRecord applies a set of filters to a record batch using Arrow Compute.
func (s *VectorStore) filterRecord(ctx context.Context, rec arrow.RecordBatch, filters []Filter) (arrow.RecordBatch, error) {
	if len(filters) == 0 {
		rec.Retain()
		return rec, nil
	}

	var mask *array.Boolean

	for _, f := range filters {
		indices := rec.Schema().FieldIndices(f.Field)
		if len(indices) == 0 {
			return nil, fmt.Errorf("field %s not found in schema", f.Field)
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
				return nil, fmt.Errorf("invalid int64 value for field %s: %w", f.Field, err)
			}
			valScalar = scalar.NewInt64Scalar(v)
		case arrow.TIMESTAMP:
			t, err := time.Parse(time.RFC3339, f.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp value for field %s: %w", f.Field, err)
			}
			ts, _ := arrow.TimestampFromTime(t, col.DataType().(*arrow.TimestampType).Unit)
			valScalar = scalar.NewTimestampScalar(ts, col.DataType().(*arrow.TimestampType))
		case arrow.FLOAT32, arrow.FLOAT64:
			v, err := strconv.ParseFloat(f.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float value for field %s: %w", f.Field, err)
			}
			if col.DataType().ID() == arrow.FLOAT32 {
				valScalar = scalar.NewFloat32Scalar(float32(v))
			} else {
				valScalar = scalar.NewFloat64Scalar(v)
			}
		default:
			return nil, fmt.Errorf("unsupported data type %s for field %s", col.DataType().Name(), f.Field)
		}

		var fn string
		switch f.Operator {
		case "=":
			fn = "equal"
		case "!=":
			fn = "not_equal"
		case ">":
			fn = "greater"
		case "<":
			fn = "less"
		case ">=":
			fn = "greater_equal"
		case "<=":
			fn = "less_equal"
		default:
			return nil, fmt.Errorf("unsupported operator %s", f.Operator)
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

// castRecordToSchema projects a record to the target schema.
func (s *VectorStore) castRecordToSchema(rec arrow.RecordBatch, targetSchema *arrow.Schema) (arrow.RecordBatch, error) {
	if rec.Schema().Equal(targetSchema) {
		rec.Retain()
		return rec, nil
	}

	cols := make([]arrow.Array, len(targetSchema.Fields()))

	for i, field := range targetSchema.Fields() {
		indices := rec.Schema().FieldIndices(field.Name)
		if len(indices) > 0 {
			srcCol := rec.Column(indices[0])
			if !arrow.TypeEqual(srcCol.DataType(), field.Type) {
				for j := 0; j < i; j++ {
					if cols[j] != nil {
						cols[j].Release()
					}
				}
				return nil, fmt.Errorf("field %s type mismatch: expected %s, got %s", field.Name, field.Type, srcCol.DataType())
			}
			srcCol.Retain()
			cols[i] = srcCol
		} else {
			bldr := array.NewBuilder(s.mem, field.Type)
			bldr.AppendNulls(int(rec.NumRows()))
			cols[i] = bldr.NewArray()
			bldr.Release()
		}
	}

	out := array.NewRecordBatch(targetSchema, cols, rec.NumRows())
	for _, c := range cols {
		if c != nil {
			c.Release()
		}
	}

	return out, nil
}

// HybridSearch is a wrapper for the HybridSearch function
func (s *VectorStore) HybridSearch(ctx context.Context, name string, query []float32, k int, filters map[string]string) ([]SearchResult, error) {
	return HybridSearch(ctx, s, name, query, k, filters)
}

// SearchHybrid is a wrapper for the SearchHybrid function (RRF version)
func (s *VectorStore) SearchHybrid(ctx context.Context, name string, query []float32, textQuery string, k int, alpha float32, rrfK int) ([]SearchResult, error) {
	return SearchHybrid(ctx, s, name, query, textQuery, k, alpha, rrfK)
}

// StoreRecordBatch stores a batch of records in a dataset
func (s *VectorStore) StoreRecordBatch(ctx context.Context, name string, rec arrow.RecordBatch) error {
	s.mu.Lock()
	ds, ok := s.datasets[name]
	if !ok {
		ds = NewDataset(name, rec.Schema())
		ds.Topo = s.numaTopology
		s.datasets[name] = ds
	}
	s.mu.Unlock()

	// WAL write
	if err := s.writeToWAL(rec, name); err != nil {
		return err
	}

	// Memory append
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	rec.Retain()
	ds.dataMu.Unlock()

	// Memory tracking
	size := estimateBatchSize(rec)
	ds.SizeBytes.Add(size)
	s.currentMemory.Add(size)

	// Inverted index update (Hybrid)
	s.indexTextColumnsForHybridSearch(rec, 0) // Simplified baseRowID for now

	// Compaction trigger
	if s.compactionWorker != nil {
		ds.dataMu.RLock()
		numBatches := len(ds.Records)
		ds.dataMu.RUnlock()
		if numBatches >= s.compactionConfig.MinBatchesToCompact {
			_ = s.compactionWorker.TriggerCompaction(name)
		}
	}

	return nil
}
