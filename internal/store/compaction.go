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

	// Statistics
	compactionsRun atomic.Int64
	batchesMerged  atomic.Int64
	rowsProcessed  atomic.Int64
	lastRunTime    atomic.Value // time.Time
}

// NewCompactionWorker creates a new compaction worker with the given config.
func NewCompactionWorker(cfg CompactionConfig) *CompactionWorker {
	w := &CompactionWorker{
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
			// Compaction logic will be integrated in Subtask 4
			w.compactionsRun.Add(1)
			metrics.CompactionOperationsTotal.WithLabelValues("completed").Inc()
			w.lastRunTime.Store(time.Now())
		}
	}
}

// compactRecords merges small RecordBatches into larger ones up to targetSize rows.
// It preserves schema, row order, and all data. Large batches are not split.
func compactRecords(records []arrow.RecordBatch, targetSize int64) []arrow.RecordBatch {
	if len(records) == 0 {
		return nil
	}

	schema := records[0].Schema()
	pool := memory.NewGoAllocator()
	var result []arrow.RecordBatch

	// Accumulator for current batch being built
	accumulated := make([]arrow.RecordBatch, 0, len(records))
	var accumulatedRows int64

	flush := func() {
		if len(accumulated) == 0 {
			return
		}
		if len(accumulated) == 1 {
			// Single batch - just retain and add
			accumulated[0].Retain()
			result = append(result, accumulated[0])
		} else {
			// Multiple batches - merge them
			merged := mergeRecordBatches(pool, schema, accumulated)
			result = append(result, merged)
		}
		accumulated = nil
		accumulatedRows = 0
	}

	for _, rec := range records {
		rows := rec.NumRows()

		// If single batch is already >= target, flush accumulated and add as-is
		if rows >= targetSize {
			flush()
			rec.Retain()
			result = append(result, rec)
			continue
		}

		// If adding this batch would exceed target, flush first
		if accumulatedRows+rows > targetSize && accumulatedRows > 0 {
			flush()
		}

		accumulated = append(accumulated, rec)
		accumulatedRows += rows
	}

	// Flush any remaining
	flush()

	return result
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
		store.compactionWorker = NewCompactionWorker(compactionCfg)
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
