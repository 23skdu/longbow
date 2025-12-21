package store

import (
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAutoSharding_Trigger(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create dataset
	// use makeHNSWTestRecord from hnsw_test.go (same package)
	// Create 2 batches of 10 vectors each
	vectors1 := make([][]float32, 10)
	vectors2 := make([][]float32, 10)
	for i := 0; i < 10; i++ {
		vectors1[i] = []float32{float32(i), float32(i), float32(i), float32(i)}
		vectors2[i] = []float32{float32(i + 10), float32(i + 10), float32(i + 10), float32(i + 10)}
	}

	rec1 := makeHNSWTestRecord(mem, 4, vectors1)
	defer rec1.Release()

	rec2 := makeHNSWTestRecord(mem, 4, vectors2)
	defer rec2.Release()

	ds := &Dataset{
		Records: nil, // Will append
		dataMu:  sync.RWMutex{},
	}

	// Append rec1
	ds.Records = append(ds.Records, rec1)

	// Config with low threshold
	config := AutoShardingConfig{
		ShardThreshold: 5, // Should trigger after 5 items
		ShardCount:     2,
	}

	idx := NewAutoShardingIndex(ds, config)

	// Add first 5 vectors (rec1, row 0-4)
	for i := 0; i < 5; i++ {
		err := idx.AddByLocation(0, i)
		require.NoError(t, err)
	}

	// Should NOT be sharded yet (threshold is inclusive or >? Impl says >= threshold triggers logic if !sharded)
	// Check impl: if !sharded && currentLen >= threshold -> migrate.
	// Current len is 5. Threshold is 5.
	// It MIGHT have migrated. Let's check internal state.
	// Access via reflection or unexported field?
	// We are in same package, so we can access `idx.sharded` directly!

	// Wait a tiny bit for async dispatch if it was async?
	// The implementation calls migrate synchronously.
	// But check logic: AddByLocation -> Add -> unlock. Then checks.

	// After 5 items, len is 5. 5 >= 5 is true.
	// So it should have migrated.

	assert.True(t, idx.sharded, "Index should be sharded after hitting threshold")
	_, ok := idx.current.(*ShardedHNSW)
	assert.True(t, ok, "Underlying index should be ShardedHNSW")

	// Add remaining vectors from rec1 (5-9)
	for i := 5; i < 10; i++ {
		err := idx.AddByLocation(0, i)
		require.NoError(t, err)
	}

	// Add rec2 (rows 0-9, batch 1)
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec2)
	ds.dataMu.Unlock()

	for i := 0; i < 10; i++ {
		err := idx.AddByLocation(1, i)
		require.NoError(t, err)
	}

	// Verify count
	assert.Equal(t, 20, idx.Len())

	// Search
	query := []float32{0, 0, 0, 0} // Closest to 0
	results := idx.SearchVectors(query, 5, nil)

	assert.NotEmpty(t, results)
	assert.Equal(t, VectorID(0), results[0].ID) // Vector 0 is {0,0,0,0} distance 0
}

func TestAutoSharding_Concurrency(t *testing.T) {
	mem := memory.NewGoAllocator()
	numVectors := 100
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		val := float32(i)
		vectors[i] = []float32{val, val, val, val}
	}

	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}

	config := AutoShardingConfig{
		ShardThreshold: 50,
		ShardCount:     4,
	}

	idx := NewAutoShardingIndex(ds, config)

	var wg sync.WaitGroup

	// Writers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numVectors; i++ {
			err := idx.AddByLocation(0, i)
			assert.NoError(t, err)
			time.Sleep(100 * time.Microsecond) // Simulate work
		}
	}()

	// Readers
	wg.Add(1)
	go func() {
		defer wg.Done()
		query := []float32{50, 50, 50, 50}
		for i := 0; i < 50; i++ {
			res := idx.SearchVectors(query, 5, nil)
			if len(res) > 0 {
				// Just verification that we don't panic
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	wg.Wait()

	assert.True(t, idx.sharded)
	assert.Equal(t, numVectors, idx.Len())
}

func TestAutoSharding_AddByRecord(t *testing.T) {
	// Verify AddByRecord also triggers migration
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1, 1, 1, 1}, {2, 2, 2, 2}}
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: nil,
	}
	// We need dataset to hold records for migration later
	// But AddByRecord implies we might add to dataset first or wrapper does it?
	// Store logic: Store adds to dataset, then calls Index.AddByRecord.
	// Index.AddByRecord usually adds to its own locations.
	// AutoShardingIndex migration reads from dataset based on locations.
	// So Dataset MUST contain the record if we want migration to work via AddByLocation.
	// But AddByRecord in HNSWIndex uses the record passed in.

	// Wait! If HNSWIndex.AddByRecord is used, it stores Location{BatchIdx, RowIdx}.
	// If those indices point to valid data in `ds`, migration works.
	// If `ds` is empty, migration fails (segfault or error).

	// So for AutoSharding to work, the Dataset passed to it MUST be updated with the records
	// referenced by AddByRecord calls.

	ds.Records = append(ds.Records, rec)

	config := AutoShardingConfig{
		ShardThreshold: 1, // Trigger immediately after 1st
	}
	idx := NewAutoShardingIndex(ds, config)

	// Add 1st
	err := idx.AddByRecord(rec, 0, 0)
	require.NoError(t, err)

	assert.True(t, idx.sharded)
}
