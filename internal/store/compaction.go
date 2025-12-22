package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// CompactionConfig configures the background RecordBatch compaction worker.
type CompactionConfig struct {
	// TargetBatchSize is the target number of rows per compacted batch.
	TargetBatchSize int64
	// MinBatchesToCompact is the minimum number of batches before compaction triggers.
	MinBatchesToCompact int
	// CompactionInterval is how often the compaction worker checks for work.
	CompactionInterval time.Duration
	// Enabled controls whether background compaction runs.
	Enabled bool
}

// DefaultCompactionConfig returns sensible defaults for compaction.
func DefaultCompactionConfig() CompactionConfig {
	return CompactionConfig{
		TargetBatchSize:     10000,
		MinBatchesToCompact: 10,
		CompactionInterval:  30 * time.Second,
		Enabled:             true,
	}
}

// Validate checks if the configuration is valid.
func (c CompactionConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.TargetBatchSize <= 0 {
		return errors.New("compaction: TargetBatchSize must be positive")
	}
	if c.MinBatchesToCompact <= 0 {
		return errors.New("compaction: MinBatchesToCompact must be positive")
	}
	if c.CompactionInterval <= 0 {
		return errors.New("compaction: CompactionInterval must be positive")
	}
	return nil
}

// CompactionStats tracks compaction worker statistics.
type CompactionStats struct {
	CompactionsRun int64
	BatchesMerged  int64
	RowsProcessed  int64
	LastRunTime    time.Time
}

// CompactionWorker runs background compaction of RecordBatches.
type CompactionWorker struct {
	config  CompactionConfig
	running atomic.Bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	mu      sync.Mutex

	// Trigger channel for auto-compaction
	triggerChan  chan string
	triggerCount atomic.Int64

	store *VectorStore

	// Statistics
	compactionsRun atomic.Int64
	batchesMerged  atomic.Int64
	rowsProcessed  atomic.Int64
	lastRunTime    atomic.Value // time.Time
}

// NewCompactionWorker creates a new compaction worker with the given config.
func NewCompactionWorker(store *VectorStore, cfg CompactionConfig) *CompactionWorker {
	w := &CompactionWorker{
		store:       store,
		config:      cfg,
		triggerChan: make(chan string, 100), // Buffered to avoid blocking
	}
	w.lastRunTime.Store(time.Time{})
	return w
}

// Start begins the background compaction goroutine.
func (w *CompactionWorker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.config.Enabled {
		return
	}
	if w.running.Load() {
		return // Already running
	}

	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.running.Store(true)

	go w.run()
}

// Stop halts the background compaction goroutine.
func (w *CompactionWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running.Load() {
		return
	}

	close(w.stopCh)
	<-w.doneCh
	w.running.Store(false)
}

// IsRunning returns true if the worker is currently running.
func (w *CompactionWorker) IsRunning() bool {
	return w.running.Load()
}

// Stats returns current compaction statistics.
func (w *CompactionWorker) Stats() CompactionStats {
	lastRun, _ := w.lastRunTime.Load().(time.Time)
	return CompactionStats{
		CompactionsRun: w.compactionsRun.Load(),
		BatchesMerged:  w.batchesMerged.Load(),
		RowsProcessed:  w.rowsProcessed.Load(),
		LastRunTime:    lastRun,
	}
}

// run is the main worker loop.
func (w *CompactionWorker) run() {
	defer close(w.doneCh)

	ticker := time.NewTicker(w.config.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			// Periodic compaction check for all datasets
			if w.store == nil {
				continue
			}
			w.store.IterateDatasets(func(ds *Dataset) {
				// Non-blocking attempt to compact
				if err := w.store.CompactDataset(ds.Name); err == nil {
					w.compactionsRun.Add(1)
					metrics.CompactionOperationsTotal.WithLabelValues("periodic").Inc()
				}
			})
			w.lastRunTime.Store(time.Now())
		case dsName := <-w.triggerChan:
			// Triggered compaction
			if w.store == nil {
				continue
			}
			if err := w.store.CompactDataset(dsName); err == nil {
				w.compactionsRun.Add(1)
				metrics.CompactionOperationsTotal.WithLabelValues("triggered").Inc()
			}
			w.lastRunTime.Store(time.Now())
		}
	}
}

// CompactionCandidate represents a range of batches to be merged
type CompactionCandidate struct {
	StartIdx int
	EndIdx   int // Exclusive
	TotalRow int64
}

// identifyCompactionCandidates finds contiguous runs of small batches.
func identifyCompactionCandidates(records []arrow.RecordBatch, targetSize int64) []CompactionCandidate {
	var candidates []CompactionCandidate
	if len(records) < 2 {
		return candidates
	}

	startIdx := 0
	currentRows := int64(0)
	count := 0

	// Greedy scan
	for i, rec := range records {
		rows := rec.NumRows()

		// If a single batch is already large enough, it acts as a barrier
		// Flush current accumulation if any
		if rows >= targetSize {
			if count > 1 {
				candidates = append(candidates, CompactionCandidate{
					StartIdx: startIdx,
					EndIdx:   i,
					TotalRow: currentRows,
				})
			}
			// Reset
			startIdx = i + 1
			currentRows = 0
			count = 0
			continue
		}

		// Accumulate
		// Look ahead: if adding this batch exceeds target significantly, maybe split?
		// For now simple greedy: accumulate until >= target
		currentRows += rows
		count++

		if currentRows >= targetSize {
			// Found a group
			if count > 1 { // Only merge if we actually combining multiple
				candidates = append(candidates, CompactionCandidate{
					StartIdx: startIdx,
					EndIdx:   i + 1,
					TotalRow: currentRows,
				})
			}
			// Reset, start fresh from next
			startIdx = i + 1
			currentRows = 0
			count = 0
		}
	}

	// Flush remaining tail if it has multiple batches
	// Even if it's small, we merge small tails to reduce fragmentation
	if count > 1 {
		candidates = append(candidates, CompactionCandidate{
			StartIdx: startIdx,
			EndIdx:   len(records),
			TotalRow: currentRows,
		})
	}

	return candidates
}

// batchRemapInfo describes how a batch ID maps to a new one
type batchRemapInfo struct {
	NewBatchIdx int
	RowOffset   int // For merged batches, where does this batch start in the new one?
}

// compactRecords returns a NEW slice of RecordBatches and a remapping table.
// It DOES NOT Modify the input slice.
func compactRecords(records []arrow.RecordBatch, targetSize int64) ([]arrow.RecordBatch, map[int]batchRemapInfo, error) {
	candidates := identifyCompactionCandidates(records, targetSize)
	if len(candidates) == 0 {
		return nil, nil, nil // Nothing to do
	}

	// We process candidates in order.
	// Since we are building a new list, we can copy untouched batches and merge candidates.

	result := make([]arrow.RecordBatch, 0, len(records))
	remapping := make(map[int]batchRemapInfo)

	currentOldIdx := 0
	pool := memory.NewGoAllocator() // TODO: Use store allocator

	for _, cand := range candidates {
		// 1. Copy untouched batches before this candidate
		for i := currentOldIdx; i < cand.StartIdx; i++ {
			rec := records[i]
			rec.Retain()
			remapping[i] = batchRemapInfo{NewBatchIdx: len(result), RowOffset: 0}
			result = append(result, rec)
		}

		// 2. Merge candidate batches
		subset := records[cand.StartIdx:cand.EndIdx]
		if len(subset) == 0 {
			continue // Should not happen
		}

		// Check schema consistency (sanity)
		schema := subset[0].Schema()
		merged := mergeRecordBatches(pool, schema, subset)

		newBatchIdx := len(result)
		result = append(result, merged)

		// 3. Record remapping for the merged batches
		currentRowOffset := 0
		for i := cand.StartIdx; i < cand.EndIdx; i++ {
			remapping[i] = batchRemapInfo{NewBatchIdx: newBatchIdx, RowOffset: currentRowOffset}
			currentRowOffset += int(records[i].NumRows())
		}

		currentOldIdx = cand.EndIdx
	}

	// 4. Copy remaining untouched batches
	for i := currentOldIdx; i < len(records); i++ {
		rec := records[i]
		rec.Retain()
		remapping[i] = batchRemapInfo{NewBatchIdx: len(result), RowOffset: 0}
		result = append(result, rec)
	}

	return result, remapping, nil
}

// mergeRecordBatches combines multiple RecordBatches into one.
func mergeRecordBatches(pool memory.Allocator, schema *arrow.Schema, batches []arrow.RecordBatch) arrow.RecordBatch {
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	for _, batch := range batches {
		for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
			col := batch.Column(colIdx)
			fieldBuilder := builder.Field(colIdx)
			appendColumn(fieldBuilder, col)
		}
	}

	return builder.NewRecordBatch()
}

// appendColumn appends all values from an arrow.Array to a builder.
func appendColumn(builder array.Builder, col arrow.Array) {
	switch b := builder.(type) {
	case *array.Int64Builder:
		arr := col.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.Float64Builder:
		arr := col.(*array.Float64)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.Float32Builder:
		arr := col.(*array.Float32)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.Int32Builder:
		arr := col.(*array.Int32)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.StringBuilder:
		arr := col.(*array.String)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.BooleanBuilder:
		arr := col.(*array.Boolean)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.FixedSizeBinaryBuilder:
		arr := col.(*array.FixedSizeBinary)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.TimestampBuilder:
		arr := col.(*array.Timestamp)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr.Value(i))
			}
		}
	case *array.FixedSizeListBuilder:
		arr := col.(*array.FixedSizeList)
		values := arr.ListValues().(*array.Float32)
		size := int(arr.DataType().(*arrow.FixedSizeListType).Len())
		valBldr := b.ValueBuilder().(*array.Float32Builder)

		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(true)
				start := i * size
				end := start + size
				for j := start; j < end; j++ {
					if values.IsNull(j) {
						valBldr.AppendNull()
					} else {
						valBldr.Append(values.Value(j))
					}
				}
			}
		}
	}
}

// TriggerCompaction triggers compaction for a specific dataset.
// This is non-blocking - if the channel is full, it returns without blocking.
func (w *CompactionWorker) TriggerCompaction(dataset string) error {
	if !w.config.Enabled {
		return nil
	}
	if !w.running.Load() {
		return nil
	}

	// Non-blocking send - drop if buffer full
	select {
	case w.triggerChan <- dataset:
		w.triggerCount.Add(1)
		metrics.CompactionAutoTriggersTotal.Inc()
	default:
		// Channel full, skip this trigger (debounce)
	}
	return nil
}

// GetTriggerCount returns the total number of auto-compaction triggers.
func (w *CompactionWorker) GetTriggerCount() int64 {
	return w.triggerCount.Load()
}

// NewVectorStoreWithCompaction returns a VectorStore with initialized compaction
func NewVectorStoreWithCompaction(mem memory.Allocator, logger *zap.Logger, maxMemoryBytes int64, walMaxBytes int64, ttlDuration time.Duration, compactionCfg CompactionConfig) *VectorStore {
	store := NewVectorStore(mem, logger, maxMemoryBytes, walMaxBytes, ttlDuration)
	store.stopCompaction() // Stop default if started
	store.compactionConfig = compactionCfg
	if compactionCfg.Enabled {
		store.compactionWorker = NewCompactionWorker(store, compactionCfg)
		store.compactionWorker.Start()
	}
	return store
}

// GetAutoCompactionTriggerCount returns trigger count from the worker
func (s *VectorStore) GetAutoCompactionTriggerCount() int64 {
	if s.compactionWorker != nil {
		return s.compactionWorker.GetTriggerCount()
	}
	return 0
}
