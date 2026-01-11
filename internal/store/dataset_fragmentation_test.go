package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataset_FragmentationTracking(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	ds := NewDataset("test-dataset", schema)
	require.NotNil(t, ds.fragmentationTracker, "Fragmentation tracker should be initialized")

	// Add a batch
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	// Add 100 records
	for i := 0; i < 100; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		vb := b.Field(1).(*array.FixedSizeListBuilder)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{float32(i), float32(i)}, nil)
	}

	rec := b.NewRecord()
	ds.Records = append(ds.Records, rec)

	// Update batch size
	ds.UpdateBatchSize(0, 100)

	// Record deletions (25%)
	for i := 0; i < 25; i++ {
		ds.RecordBatchDeletion(0)
	}

	// Get fragmented batches with 20% threshold
	fragmented := ds.GetFragmentedBatches(0.20)
	assert.Contains(t, fragmented, 0, "Batch 0 should be fragmented (25% > 20%)")

	// Reset after compaction
	ds.ResetBatchFragmentation(0)

	fragmented = ds.GetFragmentedBatches(0.20)
	assert.NotContains(t, fragmented, 0, "Batch 0 should not be fragmented after reset")
}

func TestDataset_FragmentationMultipleBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ds := NewDataset("test-dataset", schema)

	pool := memory.NewGoAllocator()

	// Add 3 batches with different sizes
	for batchIdx := 0; batchIdx < 3; batchIdx++ {
		b := array.NewRecordBuilder(pool, schema)

		size := 100 * (batchIdx + 1) // 100, 200, 300
		for i := 0; i < size; i++ {
			b.Field(0).(*array.Int64Builder).Append(int64(i))
		}

		rec := b.NewRecord()
		ds.Records = append(ds.Records, rec)
		b.Release()

		ds.UpdateBatchSize(batchIdx, size)
	}

	// Batch 0: 30% deleted (30/100)
	for i := 0; i < 30; i++ {
		ds.RecordBatchDeletion(0)
	}

	// Batch 1: 10% deleted (20/200)
	for i := 0; i < 20; i++ {
		ds.RecordBatchDeletion(1)
	}

	// Batch 2: 40% deleted (120/300)
	for i := 0; i < 120; i++ {
		ds.RecordBatchDeletion(2)
	}

	// Get fragmented batches (threshold 20%)
	fragmented := ds.GetFragmentedBatches(0.20)

	assert.Len(t, fragmented, 2, "Should have 2 fragmented batches")
	assert.Contains(t, fragmented, 0, "Batch 0 should be fragmented (30%)")
	assert.Contains(t, fragmented, 2, "Batch 2 should be fragmented (40%)")
	assert.NotContains(t, fragmented, 1, "Batch 1 should not be fragmented (10%)")
}

func TestDataset_FragmentationNilTracker(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	ds := NewDataset("test-dataset", schema)
	ds.fragmentationTracker = nil // Simulate nil tracker

	// Should not panic
	ds.UpdateBatchSize(0, 100)
	ds.RecordBatchDeletion(0)
	fragmented := ds.GetFragmentedBatches(0.20)
	ds.ResetBatchFragmentation(0)

	assert.Nil(t, fragmented, "Should return nil when tracker is nil")
}
