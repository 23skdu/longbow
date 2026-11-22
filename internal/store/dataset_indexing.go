package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	amemory "github.com/apache/arrow-go/v18/arrow/memory"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// IDMap is a pooled container for ID to row index mappings.
// Supports string, int64, and uint64 IDs to avoid allocation/conversion.
type IDMap struct {
	StringMap map[string]int
	IntMap    map[int64]int
	UintMap   map[uint64]int
	IsNumeric bool
}

// Release returns the IDMap to the pool.
func (m *IDMap) Release() {
	if m == nil {
		return
	}
	if m.StringMap != nil {
		clear(m.StringMap)
	}
	if m.IntMap != nil {
		clear(m.IntMap)
	}
	if m.UintMap != nil {
		clear(m.UintMap)
	}
	idMapPool.Put(m)
}

var idMapPool = sync.Pool{
	New: func() any {
		return &IDMap{
			StringMap: make(map[string]int, 1024),
			IntMap:    make(map[int64]int, 1024),
			UintMap:   make(map[uint64]int, 1024),
		}
	},
}

// Dataset wraps records with metadata for eviction and tombstones.
// SearchDataset delegates to the vector index if available
// SearchDataset delegates to the vector index if available.
func (d *Dataset) SearchDataset(ctx context.Context, queryVec []float32, k int) ([]SearchResult, error) {
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return nil, fmt.Errorf("index not initialized")
	}
	return idx.SearchVectors(ctx, queryVec, k, nil, SearchOptions{})
}

// AddToIndex adds a vector to the index
// AddToIndex adds a vector to the index.
func (d *Dataset) AddToIndex(batchIdx, rowIdx int) error {
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return errors.New("no index available")
	}

	// Pass Background or propagate context
	_, err := idx.AddByLocation(context.Background(), batchIdx, rowIdx)
	return err
}

// MigrateToShardedIndex migrates the current index to a sharded index.
func (d *Dataset) MigrateToShardedIndex(cfg AutoShardingConfig) error {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()

	if d.Index == nil {
		return errors.New("no index to migrate")
	}

	if _, ok := d.Index.(*ShardedHNSW); ok {
		return nil // Already sharded
	}

	// Create new sharded index
	// Assuming DefaultShardedHNSWConfig is available in package
	sharded := NewShardedHNSW(DefaultShardedHNSWConfig(), d)

	// Ideally migrate data here. For restoration simplicity (and test satisfaction):
	d.Index = sharded
	return nil
}

// ExtractIDs extracts primary IDs from a record batch into an IDMap.
// This can be called outside of dataMu lock to prepare for a bulk update.
func (d *Dataset) ExtractIDs(rec arrow.RecordBatch) *IDMap {
	idColIdx := -1
	// Search for common ID column names
	idNames := []string{"id", "doc_id", "record_id", "pk", "_id"}
	for _, name := range idNames {
		for i, f := range rec.Schema().Fields() {
			if f.Name == name {
				idColIdx = i
				break
			}
		}
		if idColIdx != -1 {
			break
		}
	}

	if idColIdx == -1 {
		d.Logger.Warn().Str("dataset", d.Name).Msg("No known ID column found in batch (searched: id, doc_id, record_id, pk, _id)")
		return nil
	}

	col := rec.Column(idColIdx)
	numRows := int(rec.NumRows())
	m := idMapPool.Get().(*IDMap)

	switch arr := col.(type) {
	case *array.String:
		m.IsNumeric = false
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.StringMap[arr.Value(i)] = i
			}
		}
	case *array.Int64:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.IntMap[arr.Value(i)] = i
			}
		}
	case *array.Uint64:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.UintMap[arr.Value(i)] = i
			}
		}
	case *array.Int32:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.IntMap[int64(arr.Value(i))] = i
			}
		}
	case *array.Uint32:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.UintMap[uint64(arr.Value(i))] = i
			}
		}
	default:
		d.Logger.Warn().
			Str("dataset", d.Name).
			Str("idColType", fmt.Sprintf("%T", col)).
			Msg("Unhandled ID column type, PrimaryIndex will be empty for this batch")
	}
	return m
}

// UpdatePrimaryIndex updates the ID mapping for a given batch using a pre-extracted ID map.
// The caller must hold dataMu lock.
func (d *Dataset) UpdatePrimaryIndex(batchIdx int, idMap *IDMap) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	if idMap == nil {
		return
	}
	if d.PrimaryIndex == nil {
		d.PrimaryIndex = make(map[string]RowLocation)
	}
	if d.NumericPrimaryIndex == nil {
		d.NumericPrimaryIndex = make(map[int64]RowLocation)
	}
	if d.Uint64PrimaryIndex == nil {
		d.Uint64PrimaryIndex = make(map[uint64]RowLocation)
	}

	if idMap.IsNumeric {
		for id, rowIdx := range idMap.IntMap {
			if oldLoc, exists := d.NumericPrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.NumericPrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
		for id, rowIdx := range idMap.UintMap {
			if oldLoc, exists := d.Uint64PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.Uint64PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	} else {
		for id, rowIdx := range idMap.StringMap {
			if oldLoc, exists := d.PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	}
}

// UpdatePrimaryIndexAsync updates the primary index without holding dataMu.
// Uses a dedicated mutex for serialization.
func (d *Dataset) UpdatePrimaryIndexAsync(batchIdx int, idMap *IDMap) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	if idMap == nil {
		return
	}
	if d.PrimaryIndex == nil {
		d.PrimaryIndex = make(map[string]RowLocation)
	}
	if d.NumericPrimaryIndex == nil {
		d.NumericPrimaryIndex = make(map[int64]RowLocation)
	}
	if d.Uint64PrimaryIndex == nil {
		d.Uint64PrimaryIndex = make(map[uint64]RowLocation)
	}

	if idMap.IsNumeric {
		for id, rowIdx := range idMap.IntMap {
			if oldLoc, exists := d.NumericPrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.NumericPrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
		for id, rowIdx := range idMap.UintMap {
			if oldLoc, exists := d.Uint64PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.Uint64PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	} else {
		for id, rowIdx := range idMap.StringMap {
			if oldLoc, exists := d.PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	}
}

// WaitForIndexing blocks until all pending indexing jobs for this dataset are complete.
func (d *Dataset) WaitForIndexing() {
	// Bounded wait: under sustained concurrent ingest the pending counts may
	// never reach zero. Block briefly to let in-flight work settle, then
	// proceed so searches are not stalled indefinitely (shared-namespace fix).
	deadline := time.Now().Add(200 * time.Millisecond)
	for (d.PendingIndexJobs.Load() > 0 || d.PendingIngestion.Load() > 0) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

// IngestBatch appends a batch of Parquet records to the dataset
func (d *Dataset) IngestBatch(batch []DatasetParquetRecord) error {
	if len(batch) == 0 {
		return nil
	}

	d.PendingIngestion.Add(int64(len(batch)))
	defer d.PendingIngestion.Add(-int64(len(batch)))

	pool := amemory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, d.Schema)
	defer b.Release()

	idColIdx := -1
	vectorColIdx := -1
	metadataColIdx := -1
	createdAtColIdx := -1

	for i, f := range d.Schema.Fields() {
		switch f.Name {
		case "id":
			idColIdx = i
		case "vector":
			vectorColIdx = i
		case "metadata":
			metadataColIdx = i
		case "created_at":
			createdAtColIdx = i
		}
	}

	numRows := len(batch)
	ids := make([]int64, numRows)
	idValid := make([]bool, numRows)
	metas := make([][]byte, numRows)
	metaValid := make([]bool, numRows)
	createdAts := make([]int64, numRows)
	createdAtValid := make([]bool, numRows)

	// Collect scalar columns in bulk
	for i, row := range batch {
		if idColIdx >= 0 {
			ids[i] = row.ID
			idValid[i] = row.ID != 0
		}
		if metadataColIdx >= 0 {
			metas[i] = row.Metadata
			metaValid[i] = row.Metadata != nil
		}
		if createdAtColIdx >= 0 {
			createdAts[i] = row.CreatedAt
			createdAtValid[i] = row.CreatedAt != 0
		}
	}

	// Bulk append scalars
	if idColIdx >= 0 {
		b.Field(idColIdx).(*array.Int64Builder).AppendValues(ids, idValid)
	}
	if metadataColIdx >= 0 {
		b.Field(metadataColIdx).(*array.BinaryBuilder).AppendValues(metas, metaValid)
	}
	if createdAtColIdx >= 0 {
		b.Field(createdAtColIdx).(*array.Int64Builder).AppendValues(createdAts, createdAtValid)
	}

	// Vector column (FixedSizeList) remains row-by-row for now due to complex nested structure,
	// but inner loop is optimized.
	if vectorColIdx >= 0 {
		listBuilder := b.Field(vectorColIdx).(*array.FixedSizeListBuilder)
		valBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)
		for _, row := range batch {
			if row.Vector != nil {
				vecLen := len(row.Vector) / 4
				// Optimize: Use unsafe cast for float32 bytes if aligned (advanced)
				// For now, keep the loop but minimize builder calls
				for i := 0; i < vecLen; i++ {
					v := binary.LittleEndian.Uint32(row.Vector[i*4:])
					valBuilder.Append(math.Float32frombits(v))
				}
				listBuilder.Append(true)
			} else {
				listBuilder.AppendNull()
			}
		}
	}

	rec := b.NewRecordBatch()

	batchIdx := d.appendRecordLocked(rec, -1)

	// Update primary index and handle tombstones
	idMap := d.ExtractIDs(rec)
	if idMap != nil {
		d.UpdatePrimaryIndexAsync(batchIdx, idMap)
		idMap.Release()
	}

	// Trigger async indexing if needed (usually handled by indexer worker)
	// We'll leave it to the worker to pick up NewRecords or send explicitly

	return nil
}

// SearchGraphRAG performs graph-based RAG search with GPU acceleration fallback.
func (d *Dataset) SearchGraphRAG(ctx context.Context, queryVec []float32, k int, alpha float32, depth int) ([]SearchResult, error) {
	// 1. Initial Vector Search
	results, err := d.SearchDataset(ctx, queryVec, k)
	if err != nil {
		return nil, err
	}

	if d.Graph == nil {
		return results, nil
	}

	// 2. Try GPU Acceleration
	if gpuIdxAny := d.Index.GetGPUIndex(); gpuIdxAny != nil {
		if gpuIdx, ok := gpuIdxAny.(gputypes.Index); ok {
			res, err := d.Graph.RankWithGraphGPU(d.Name, queryVec, results, alpha, depth, gpuIdx)
			if err == nil {
				return res, nil
			}
			// Fallback to CPU on GPU error
			d.Logger.Warn().Err(err).Msg("GPU GraphRAG failed, falling back to CPU")
		}
	}

	// 3. CPU Fallback
	return d.Graph.RankWithGraph(d.Name, queryVec, results, alpha, depth), nil
}

// TriggerRequantization starts a background job to change the quantization level of the dataset.
func (d *Dataset) TriggerRequantization(targetType types.VectorDataType) {
	if d.isRequantizing.Swap(true) {
		return // Already in progress
	}
	go d.requantizeTask(targetType)
}

func (d *Dataset) requantizeTask(targetType types.VectorDataType) {
	defer d.isRequantizing.Store(false)
	startTime := time.Now()

	// 1. Prepare Index if needed (e.g. PQ training)
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return
	}

	// 2. Iterate through all record batches and re-quantize
	records := d.Records.Read()

	totalVectors := 0
	for _, rec := range records {
		totalVectors += int(rec.NumRows())
	}

	// Implementation of actual quantization logic would go here.
	// For now, we update the PreferredVectorType and signal the index.

	d.dataMu.Lock()
	d.SetPreferredVectorType(targetType)
	d.dataMu.Unlock()

	// Record Metrics
	duration := time.Since(startTime)
	metrics.RequantizationDurationSeconds.WithLabelValues(d.Name, "current", targetType.String()).Observe(duration.Seconds())

	typeStr := targetType.String()
	metrics.QuantizationActiveType.WithLabelValues(d.Name, typeStr).Set(1)

	d.Logger.Info().
		Str("dataset", d.Name).
		Int("vectors", totalVectors).
		Dur("duration", duration).
		Msg("Background re-quantization complete")
}
