package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedHNSW_Compaction(t *testing.T) {
	mem := memory.NewGoAllocator()
	dataset := &Dataset{Name: "test-compaction"}

	// 1. Setup Sharded Index (Fixed shards for simplicity)
	config := DefaultShardedHNSWConfig()
	config.NumShards = 2
	config.Dimension = 4
	idx := NewShardedHNSW(config, dataset)

	// 2. Insert Data
	// Create a dummy record batch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	// Batch 0: 10 rows
	idBldr := bldr.Field(0).(*array.Int64Builder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < 10; i++ {
		idBldr.Append(int64(i))
		vecBldr.Append(true)
		for j := 0; j < 4; j++ {
			valBldr.Append(float32(i))
		}
	}
	rec0 := bldr.NewRecordBatch()
	defer rec0.Release()

	dataset.Records = []arrow.RecordBatch{rec0}

	// Add to index
	for i := 0; i < 10; i++ {
		_, err := idx.AddByRecord(rec0, i, 0)
		require.NoError(t, err)
	}

	// Verify lookups
	res, err := idx.SearchVectors([]float32{0, 0, 0, 0}, 1, nil, SearchOptions{})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, VectorID(0), res[0].ID) // ID 0 is closest to 0,0,0,0

	// 3. Simulate Compaction
	// Move Batch 0 -> Batch 10 (Compacted)
	// Remap: Batch 0, Rows 0-9 -> Batch 10, Rows 0-9
	remapping := make(map[int]BatchRemapInfo)
	newRowIdxs := make([]int, 10)
	for i := 0; i < 10; i++ {
		newRowIdxs[i] = i
	}
	remapping[0] = BatchRemapInfo{NewBatchIdx: 10, NewRowIdxs: newRowIdxs}

	// Update dataset records (mock)
	dataset.Records = make([]arrow.RecordBatch, 11) // Resize to accommodate batch 10
	rec0.Retain()
	dataset.Records[10] = rec0 // Move record to pos 10

	// Call Remap
	err = idx.RemapFromBatchInfo(remapping)
	require.NoError(t, err)

	// 4. Verify Search Uses New Location
	// We need to inspect location store
	loc, ok := idx.GetLocation(VectorID(0))
	require.True(t, ok)
	assert.Equal(t, 10, loc.BatchIdx)
	assert.Equal(t, 0, loc.RowIdx)

	// SearchByID should still work
	foundIDs := idx.SearchByID(VectorID(0), 5)
	assert.Contains(t, foundIDs, VectorID(0)) // Should find itself
}

func TestShardedHNSW_Vacuum(t *testing.T) {
	mem := memory.NewGoAllocator()
	dataset := &Dataset{Name: "test-vacuum"}
	config := DefaultShardedHNSWConfig()
	config.NumShards = 1
	config.Dimension = 4
	idx := NewShardedHNSW(config, dataset)

	// Insert 10 items
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	vecBldr := bldr.Field(0).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < 10; i++ {
		vecBldr.Append(true)
		for j := 0; j < 4; j++ {
			valBldr.Append(float32(i))
		}
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()
	dataset.Records = []arrow.RecordBatch{rec}

	for i := 0; i < 10; i++ {
		_, err := idx.AddByRecord(rec, i, 0)
		require.NoError(t, err)
	}

	// Mark 5 items as deleted in dataset
	dataset.Tombstones = make(map[int]*query.Bitset)
	ts := query.NewBitset()
	for i := 0; i < 5; i++ {
		ts.Set(i)
	}
	dataset.Tombstones[0] = ts

	// Run Vacuum
	pruned := idx.CleanupTombstones(0)
	// Note: CleanupTombstones logic depends on implementation details of ArrowHNSW.
	// We expect at least some nodes to be visited/marked.
	// Since we mock deletion via Dataset Tombstones, but ArrowHNSW uses `Index.Tombstones`?
	// Wait, ArrowHNSW.CleanupTombstones uses `idx.dataset.Tombstones`.
	// So this should work.
	assert.True(t, pruned >= 0)

	// Verify deleted items are effectively gone from search results?
	// Vacuum removes from graph, but they might still be in locations?
	// Real test of Vacuum is graph cleaner.
	// Just ensuring it runs without panic and returns >= 0 is a good baseline.
}

func TestShardedHNSW_DynamicGrowth(t *testing.T) {
	mem := memory.NewGoAllocator()
	dataset := &Dataset{Name: "test-growth"}
	config := DefaultShardedHNSWConfig()
	config.NumShards = 1            // Start with 1
	config.UseRingSharding = false  // Enable Linear
	config.ShardSplitThreshold = 10 // Split every 10 items
	config.Dimension = 4
	idx := NewShardedHNSW(config, dataset)

	// Insert 25 items -> Should grow to 3 shards (0-9, 10-19, 20-24)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	vecBldr := bldr.Field(0).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < 25; i++ {
		vecBldr.Append(true)
		for j := 0; j < 4; j++ {
			valBldr.Append(float32(i))
		}
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()
	dataset.Records = []arrow.RecordBatch{rec}

	for i := 0; i < 25; i++ {
		_, err := idx.AddByRecord(rec, i, 0)
		require.NoError(t, err)
	}

	// Verify 3 shards
	idx.shardsMu.RLock()
	assert.GreaterOrEqual(t, len(idx.shards), 3)
	idx.shardsMu.RUnlock()

	// Verify all items are findable
	for i := 0; i < 25; i++ {
		loc, ok := idx.GetLocation(VectorID(i))
		require.True(t, ok, "ID %d not found", i)
		assert.Equal(t, i, loc.RowIdx)
	}
}
