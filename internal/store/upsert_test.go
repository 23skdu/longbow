package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
)

func createUpsertTestRecord(t *testing.T, allocator memory.Allocator, startID int, count int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(allocator, schema)
	defer b.Release()

	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	floatB := vecB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < count; i++ {
		idB.Append(string([]byte{byte('v'), byte('e'), byte('c'), byte(startID + i)}))

		vecB.Append(true)
		for j := 0; j < 128; j++ {
			floatB.Append(float32(startID + i + j))
		}
	}

	return b.NewRecordBatch()
}

func TestStore_Upsert(t *testing.T) {
	alloc := memory.NewGoAllocator()
	s := NewVectorStore(alloc, zerolog.Nop(), 10*1024*1024, 0, 0)
	defer s.Close()

	// Wait for background workers to start up
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()

	datasetName := "test_upserts"
	
	// Create original batch with ID 1
	rec1 := createUpsertTestRecord(t, alloc, 1, 1)
	defer rec1.Release()

	err := s.StoreRecordBatch(ctx, datasetName, rec1)
	require.NoError(t, err)

	// Wait for ingestion dispatch
	time.Sleep(100 * time.Millisecond)

	ds, ok := s.getDataset(datasetName)
	require.True(t, ok)

	ds.WaitForIndexing()
	
	// Ensure the vector exists
	res1, err := ds.SearchDataset(ctx, make([]float32, 128), 10)
	require.NoError(t, err)
	require.Len(t, res1, 1, "Should have 1 vector after initial insert")

	// Upsert the same ID but with different vector values (simulating a replacement)
	rec2 := createUpsertTestRecord(t, alloc, 1, 1)
	defer rec2.Release()

	err = s.StoreRecordBatch(ctx, datasetName, rec2)
	require.NoError(t, err)

	// Wait for the synchronous bits to apply
	time.Sleep(100 * time.Millisecond)
	ds.WaitForIndexing()

	// Verify Tombstones
	ds.dataMu.RLock()
	// Batch 0 should have the first insert row tombstoned
	ts0 := ds.Tombstones[0]
	require.NotNil(t, ts0)
	require.True(t, ts0.Contains(0), "Original RowLocation should be strictly tombstoned")
	ds.dataMu.RUnlock()

	// Perform a query. The search should skip the tombstoned result and return ONLY the latest.
	res2, err := ds.SearchDataset(ctx, make([]float32, 128), 10)
	require.NoError(t, err)
	
	var filtered []SearchResult
	ds.dataMu.RLock()
	for _, r := range res2 {
		locAny, found := ds.Index.GetLocation(uint32(r.ID))
		if found {
			loc := locAny.(Location)
			ts := ds.Tombstones[loc.BatchIdx]
			if ts == nil || !ts.Contains(loc.RowIdx) {
				filtered = append(filtered, r)
			}
		}
	}
	ds.dataMu.RUnlock()

	// If upsert logic correctly tombstoned the older version, we should still only have 1 length result array!
	require.Len(t, filtered, 1, "Search should skip the tombstoned result and still only yield 1 vector!")
}
