package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveIndex(t *testing.T) {
	// 1. Setup Dataset with Schema
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test_adaptive", schema)

	// Add a record batch to the dataset
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	
	listBuilder := b.Field(0).(*array.FixedSizeListBuilder)
	valBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)
	
	// Row 0: [1,0,0,0]
	valBuilder.AppendValues([]float32{1, 0, 0, 0}, nil)
	listBuilder.Append(true)
	
	// Row 1: [0,1,0,0]
	valBuilder.AppendValues([]float32{0, 1, 0, 0}, nil)
	listBuilder.Append(true)
	
	rec := b.NewRecord()
	ds.Records = append(ds.Records, rec)

	// 2. Initialize AdaptiveIndex with low threshold
	cfg := AdaptiveIndexConfig{
		Threshold: 2,
		Enabled:   true,
	}
	idx := NewAdaptiveIndex(ds, cfg)
	
	assert.Equal(t, "brute_force", idx.GetIndexType())
	assert.False(t, idx.IsSharded())

	// 3. Add first vector (Brute Force)
	_, err := idx.AddByLocation(context.Background(), 0, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, idx.Len())
	assert.Equal(t, "brute_force", idx.GetIndexType())

	// 4. Search while in Brute Force
	results, err := idx.SearchVectors(context.Background(), []float32{1, 0, 0, 0}, 1, nil, SearchOptions{})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, float32(0), results[0].Score) // Score 0 for distance 0 in BF? Wait, score is distance in BF search item

	// 5. Add second vector (cross threshold -> trigger migration)
	_, err = idx.AddByLocation(context.Background(), 0, 1)
	require.NoError(t, err)
	
	// Wait for async migration
	maxWait := 5 * time.Second
	start := time.Now()
	for idx.GetIndexType() != "hnsw" && time.Since(start) < maxWait {
		time.Sleep(10 * time.Millisecond)
	}
	
	assert.Equal(t, "hnsw", idx.GetIndexType())
	assert.Equal(t, int64(1), idx.GetMigrationCount())
	assert.Equal(t, 2, idx.Len())

	// 6. Search while in HNSW
	results, err = idx.SearchVectors(context.Background(), []float32{1, 0, 0, 0}, 1, nil, SearchOptions{})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	
	// Clean up
	err = idx.Close()
	assert.NoError(t, err)
}

func TestAdaptiveIndexDisabled(t *testing.T) {
	ds := NewDataset("test_disabled", nil)
	cfg := AdaptiveIndexConfig{
		Threshold: 1,
		Enabled:   false,
	}
	idx := NewAdaptiveIndex(ds, cfg)
	
	_, _ = idx.AddByLocation(context.Background(), 0, 0)
	time.Sleep(100 * time.Millisecond)
	
	assert.Equal(t, "brute_force", idx.GetIndexType())
}
