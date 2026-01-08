package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
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
			w.store.IterateDatasets(func(name string, ds *Dataset) {
				// Non-blocking attempt to compact
				start := time.Now()
				if err := w.store.CompactDataset(ds.Name); err == nil {
					duration := time.Since(start).Seconds()
					metrics.CompactionDurationSeconds.WithLabelValues(ds.Name, "periodic").Observe(duration)
					w.compactionsRun.Add(1)
					metrics.CompactionOperationsTotal.WithLabelValues(ds.Name, "periodic").Inc()
				} else {
					metrics.CompactionErrorsTotal.Inc()
				}

				// Run Vacuum (Graph Compaction)
				// We do this separately. It handles graph edges pointing to deleted nodes.
				if err := w.store.VacuumDataset(ds.Name); err != nil {
					w.store.logger.Warn().Err(err).Str("dataset", ds.Name).Msg("Failed to vacuum dataset during compaction")
				}
			})
			w.lastRunTime.Store(time.Now())
		case dsName := <-w.triggerChan:
			// Triggered compaction
			if w.store == nil {
				continue
			}
			start := time.Now()
			if err := w.store.CompactDataset(dsName); err == nil {
				duration := time.Since(start).Seconds()
				metrics.CompactionDurationSeconds.WithLabelValues(dsName, "triggered").Observe(duration)
				w.compactionsRun.Add(1)
				metrics.CompactionOperationsTotal.WithLabelValues(dsName, "triggered").Inc()
			} else {
				metrics.CompactionErrorsTotal.Inc()
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

// BatchRemapInfo tracks how a batch moved during compaction
type BatchRemapInfo struct {
	NewBatchIdx int
	NewRowIdxs  []int
}

// compactRecords returns a NEW slice of RecordBatches and a remapping table.
// It DOES NOT Modify the input slice.
func compactRecords(pool memory.Allocator, schema *arrow.Schema, records []arrow.RecordBatch, tombstones map[int]*qry.Bitset, targetSize int64, datasetName string) ([]arrow.RecordBatch, map[int]BatchRemapInfo) {
	if schema == nil && len(records) > 0 {
		schema = records[0].Schema()
	}
	candidates := identifyCompactionCandidates(records, targetSize)
	if len(candidates) == 0 {
		return nil, nil // Nothing to do
	}

	// We process candidates in order.
	// Since we are building a new list, we can copy untouched batches and merge candidates.

	result := make([]arrow.RecordBatch, 0, len(records))
	remapping := make(map[int]BatchRemapInfo)

	currentOldIdx := 0
	// pool := memory.NewGoAllocator() // Replaced with passed-in allocator

	totalRemoved := int64(0)

	for _, cand := range candidates {
		// 1. Copy untouched batches before this candidate
		for i := currentOldIdx; i < cand.StartIdx; i++ {
			rec := records[i]
			tomb := tombstones[i]
			if tomb != nil && tomb.Count() > 0 {
				// We still need to filter this "untouched" batch because it has tombstones
				filtered, rowMapping, removed := filterTombstones(pool, schema, rec, tomb)
				totalRemoved += removed
				remapping[i] = BatchRemapInfo{NewBatchIdx: len(result), NewRowIdxs: rowMapping}
				result = append(result, filtered)
			} else {
				rec.Retain()
				rowMapping := make([]int, rec.NumRows())
				for r := 0; r < int(rec.NumRows()); r++ {
					rowMapping[r] = r
				}
				remapping[i] = BatchRemapInfo{NewBatchIdx: len(result), NewRowIdxs: rowMapping}
				result = append(result, rec)
			}
		}

		// 2. Merge candidate batches
		subset := records[cand.StartIdx:cand.EndIdx]
		if len(subset) == 0 {
			continue // Should not happen
		}

		// Collect tombstones for this candidate group
		candTombstones := make([]*qry.Bitset, len(subset))
		for i := cand.StartIdx; i < cand.EndIdx; i++ {
			candTombstones[i-cand.StartIdx] = tombstones[i]
		}

		merged, candRowMappings, removed := mergeAndFilterRecordBatches(pool, schema, subset, candTombstones)
		totalRemoved += removed

		newBatchIdx := len(result)
		result = append(result, merged)

		// 3. Record remapping for the merged batches
		for i := cand.StartIdx; i < cand.EndIdx; i++ {
			remapping[i] = BatchRemapInfo{
				NewBatchIdx: newBatchIdx,
				NewRowIdxs:  candRowMappings[i-cand.StartIdx],
			}
		}

		currentOldIdx = cand.EndIdx
	}

	// 4. Copy remaining untouched batches
	for i := currentOldIdx; i < len(records); i++ {
		rec := records[i]
		tomb := tombstones[i]
		if tomb != nil && tomb.Count() > 0 {
			filtered, rowMapping, removed := filterTombstones(pool, schema, rec, tomb)
			totalRemoved += removed
			remapping[i] = BatchRemapInfo{NewBatchIdx: len(result), NewRowIdxs: rowMapping}
			result = append(result, filtered)
		} else {
			rec.Retain()
			rowMapping := make([]int, rec.NumRows())
			for r := 0; r < int(rec.NumRows()); r++ {
				rowMapping[r] = r
			}
			remapping[i] = BatchRemapInfo{NewBatchIdx: len(result), NewRowIdxs: rowMapping}
			result = append(result, rec)
		}
	}

	if totalRemoved > 0 {
		metrics.CompactionRecordsRemovedTotal.WithLabelValues(datasetName).Add(float64(totalRemoved))
	}

	return result, remapping
}

// filterTombstones creates a new RecordBatch with deleted rows removed.
func filterTombstones(pool memory.Allocator, schema *arrow.Schema, rec arrow.RecordBatch, tomb *qry.Bitset) (arrow.RecordBatch, []int, int64) {
	numRows := int(rec.NumRows())
	rowMapping := make([]int, numRows)
	keepIndices := make([]int, 0, numRows)

	for i := 0; i < numRows; i++ {
		if tomb.Contains(i) {
			rowMapping[i] = -1
		} else {
			rowMapping[i] = len(keepIndices)
			keepIndices = append(keepIndices, i)
		}
	}

	if len(keepIndices) == numRows {
		rec.Retain()
		return rec, rowMapping, 0
	}

	if len(keepIndices) == 0 {
		// All rows deleted - return empty record with same schema
		builder := array.NewRecordBuilder(pool, schema)
		defer builder.Release()
		return builder.NewRecordBatch(), rowMapping, int64(numRows)
	}

	// Filter using Arrow compute or manual copy
	// For simplicity and to avoid context overhead in background worker, use manual builder
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	for colIdx := 0; colIdx < int(schema.NumFields()); colIdx++ {
		// Dynamic check for column existence in batch
		if colIdx >= int(rec.NumCols()) {
			fieldBuilder := builder.Field(colIdx)
			for range keepIndices {
				fieldBuilder.AppendNull()
			}
			continue
		}

		col := rec.Column(colIdx)
		fieldBuilder := builder.Field(colIdx)
		for _, rowIdx := range keepIndices {
			appendValue(fieldBuilder, col, rowIdx)
		}
	}

	return builder.NewRecordBatch(), rowMapping, int64(numRows - len(keepIndices))
}

// mergeAndFilterRecordBatches combines multiple record batches into one while skipping tombstones.
func mergeAndFilterRecordBatches(pool memory.Allocator, schema *arrow.Schema, batches []arrow.RecordBatch, tombstones []*qry.Bitset) (arrow.RecordBatch, [][]int, int64) {
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	rowMappings := make([][]int, len(batches))
	totalRemoved := int64(0)
	currentRowOffset := 0

	for i, batch := range batches {
		numRows := int(batch.NumRows())
		rowMappings[i] = make([]int, numRows)
		tomb := tombstones[i]

		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if tomb != nil && tomb.Contains(rowIdx) {
				rowMappings[i][rowIdx] = -1
				totalRemoved++
			} else {
				rowMappings[i][rowIdx] = currentRowOffset
				for colIdx := 0; colIdx < int(schema.NumFields()); colIdx++ {
					fieldBuilder := builder.Field(colIdx)
					if colIdx >= int(batch.NumCols()) {
						fieldBuilder.AppendNull()
						continue
					}
					col := batch.Column(colIdx)
					appendValue(fieldBuilder, col, rowIdx)
				}
				currentRowOffset++
			}
		}
	}

	return builder.NewRecordBatch(), rowMappings, totalRemoved
}

// appendValue appends a single value from an arrow.Array to a builder.
// appendValue appends a single value from an arrow.Array to a builder.
// It handles all supported Arrow types recursively.
func appendValue(builder array.Builder, col arrow.Array, rowIdx int) {
	if col.IsNull(rowIdx) {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if val, ok := col.(*array.Boolean); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.Int8Builder:
		switch val := col.(type) {
		case *array.Int8:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Int16Builder:
		switch val := col.(type) {
		case *array.Int16:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Int32Builder:
		switch val := col.(type) {
		case *array.Int32:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Int64Builder:
		switch val := col.(type) {
		case *array.Int64:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Uint8Builder:
		switch val := col.(type) {
		case *array.Uint8:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Uint16Builder:
		switch val := col.(type) {
		case *array.Uint16:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Uint32Builder:
		switch val := col.(type) {
		case *array.Uint32:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Uint64Builder:
		switch val := col.(type) {
		case *array.Uint64:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Float16Builder:
		switch val := col.(type) {
		case *array.Float16:
			b.Append(val.Value(rowIdx))
		default:
			builder.AppendNull()
		}
	case *array.Float32Builder:
		switch val := col.(type) {
		case *array.Float32:
			b.Append(val.Value(rowIdx))
		case *array.Float64:
			b.Append(float32(val.Value(rowIdx)))
		default:
			builder.AppendNull()
		}
	case *array.Float64Builder:
		switch val := col.(type) {
		case *array.Float64:
			b.Append(val.Value(rowIdx))
		case *array.Float32:
			b.Append(float64(val.Value(rowIdx)))
		default:
			builder.AppendNull()
		}
	case *array.StringBuilder:
		if val, ok := col.(*array.String); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.BinaryBuilder:
		if val, ok := col.(*array.Binary); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.FixedSizeBinaryBuilder:
		if val, ok := col.(*array.FixedSizeBinary); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.TimestampBuilder:
		if val, ok := col.(*array.Timestamp); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.Time32Builder:
		if val, ok := col.(*array.Time32); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.Time64Builder:
		if val, ok := col.(*array.Time64); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.Date32Builder:
		if val, ok := col.(*array.Date32); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.Date64Builder:
		if val, ok := col.(*array.Date64); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.DurationBuilder:
		if val, ok := col.(*array.Duration); ok {
			b.Append(val.Value(rowIdx))
		} else {
			builder.AppendNull()
		}
	case *array.FixedSizeListBuilder:
		arr, ok := col.(*array.FixedSizeList)
		if !ok {
			builder.AppendNull()
			return
		}

		b.Append(true)

		values := arr.ListValues()
		valBuilder := b.ValueBuilder()

		size := int(arr.DataType().(*arrow.FixedSizeListType).Len())
		start := rowIdx * size
		end := start + size

		// Recursively append inner values
		for j := start; j < end; j++ {
			appendValue(valBuilder, values, j)
		}

	case *array.ListBuilder:
		arr, ok := col.(*array.List)
		if !ok {
			builder.AppendNull()
			return
		}

		b.Append(true)

		values := arr.ListValues()
		valBuilder := b.ValueBuilder()

		start := int(arr.Offsets()[rowIdx])
		end := int(arr.Offsets()[rowIdx+1])

		for j := start; j < end; j++ {
			appendValue(valBuilder, values, j)
		}

	case *array.StructBuilder:
		arr, ok := col.(*array.Struct)
		if !ok {
			builder.AppendNull()
			return
		}

		b.Append(true)

		numFields := arr.NumField()
		for i := 0; i < numFields; i++ {
			fieldBuilder := b.FieldBuilder(i)
			fieldCol := arr.Field(i)
			appendValue(fieldBuilder, fieldCol, rowIdx)
		}

	default:
		// Fallback for unsupported types: append NULL to keep row counts strict
		// This prevents "field has 0 rows" panics but may result in data loss for unsupported types.
		// Given compaction is critical, data loss for unknown types is better than crashing.
		builder.AppendNull()
	}
}

// Old mergeRecordBatches and appendColumn are no longer used by compactRecords
// but might be used elsewhere. Keeping them unexported if needed or just removing if they were internal helpers.
// Looking at previous state, mergeRecordBatches was unexported.

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
func NewVectorStoreWithCompaction(mem memory.Allocator, logger zerolog.Logger, maxMemoryBytes int64, walMaxBytes int64, ttlDuration time.Duration, compactionCfg CompactionConfig) *VectorStore {
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
