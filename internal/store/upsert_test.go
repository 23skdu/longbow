package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func createUpsertTestRecord(allocator memory.Allocator, startID int, count int) arrow.RecordBatch {
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	alloc := memory.NewGoAllocator()
	s := NewVectorStore(alloc, zerolog.Nop(), 1024*1024*1024, 0, 0)
	s.StartIndexingWorkers(2)
	s.StartIngestionWorkers(2)
	defer s.Close()

	ctx := context.Background()
	datasetName := "test_upserts"

	// Create and store original batch with ID 1
	rec1 := createUpsertTestRecord(alloc, 1, 1)
	defer rec1.Release()

	err := s.StoreRecordBatch(ctx, datasetName, rec1)
	require.NoError(t, err)

	ds, ok := s.getDataset(datasetName)
	require.True(t, ok)
	ds.WaitForIndexing()

	// Ensure the vector exists
	res1, err := ds.SearchDataset(ctx, make([]float32, 128), 10)
	require.NoError(t, err)
	require.Len(t, res1, 1, "Should have 1 vector after initial insert")

	// Upsert: same ID, new vector values — add directly to records/index
	rec2 := createUpsertTestRecord(alloc, 1, 1)
	defer rec2.Release()

	ds.dataMu.Lock()
	batchIdx := len(ds.Records.Read())
	newRecords := append(ds.Records.Read(), rec2)
	rec2.Retain()
	ds.Records.UpdateInPlace(newRecords)
	ds.dataMu.Unlock()

	// Update primary index (tombstones old location, records new)
	ds.UpdatePrimaryIndex(batchIdx, ds.ExtractIDs(rec2))

	// Index the new vector
	_, err = ds.Index.AddByLocation(context.Background(), batchIdx, 0)
	require.NoError(t, err)

	// Search — should find both vectors; filter out tombstoned
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

	require.Len(t, filtered, 1, "Search should skip the tombstoned result and still only yield 1 vector!")
}
