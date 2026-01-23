package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/flight"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeAdaptiveTestRecord(mem memory.Allocator, size int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewInt64Builder(mem)
	defer b.Release()

	for i := int64(0); i < size; i++ {
		b.Append(i)
	}

	arr := b.NewArray()
	defer arr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{arr}, size)
}

func TestAdaptivelySliceBatches(t *testing.T) {
	mem := memory.NewGoAllocator()
	strategy := flight.NewAdaptiveChunkStrategy(10, 100, 2.0) // Start 10, double up to 100

	// Create a large record (50 rows)
	// Expected chunks: 10, 20, 20 (capped by remaining)
	rec1 := makeAdaptiveTestRecord(mem, 50)
	defer rec1.Release()

	// Create another record (25 rows) with tombstone
	// Expected chunks: 40 (capped by record size 25)
	rec2 := makeAdaptiveTestRecord(mem, 25)
	defer rec2.Release()

	// Create tombstone for rec2: mark row 5 and 20 as deleted
	ts2 := query.NewBitset()
	ts2.Set(5)
	ts2.Set(20)

	records := []arrow.RecordBatch{rec1, rec2}
	tombstones := map[int]*query.Bitset{
		1: ts2,
	}

	// Execute slicing
	slicedRecords, slicedTombstones := AdaptivelySliceBatches(records, tombstones, strategy)
	defer func() {
		for _, r := range slicedRecords {
			r.Release()
		}
	}()

	// Verify Chunking
	// 1. Rec1 (50 rows):
	//    - Chunk 1: 10 rows (Strategy start)
	//    - Chunk 2: 20 rows (Strategy 10*2)
	//    - Chunk 3: 20 rows (Remaining 50-30; Strategy is 40 but capped by remaining)
	// 2. Rec2 (25 rows):
	//    - Chunk 4: 25 rows (Strategy is 40 but capped by record size)

	require.Equal(t, 4, len(slicedRecords))

	assert.Equal(t, int64(10), slicedRecords[0].NumRows())
	assert.Equal(t, int64(20), slicedRecords[1].NumRows())
	assert.Equal(t, int64(20), slicedRecords[2].NumRows())
	assert.Equal(t, int64(25), slicedRecords[3].NumRows())

	// Verify Tombstones
	// Rec2 became Chunk 4 (index 3).
	// Row 5 in original rec2 -> Row 5 in Chunk 4.
	// Row 20 in original rec2 -> Row 20 in Chunk 4.

	tsOut, ok := slicedTombstones[3]
	require.True(t, ok, "Chunk 3 (from rec2) should have tombstones")
	assert.True(t, tsOut.Contains(5))
	assert.True(t, tsOut.Contains(20))
	assert.Equal(t, uint64(2), tsOut.Count())

	// Verify other chunks have no tombstones
	_, ok = slicedTombstones[0]
	assert.False(t, ok)
	_, ok = slicedTombstones[1]
	assert.False(t, ok)
	_, ok = slicedTombstones[2]
	assert.False(t, ok)
}

func TestAdaptivelySliceBatches_SplittingTombstones(t *testing.T) {
	mem := memory.NewGoAllocator()
	strategy := flight.NewAdaptiveChunkStrategy(10, 100, 2.0)

	// Record with 30 rows
	// Chunks: 10, 20
	rec := makeAdaptiveTestRecord(mem, 30)
	defer rec.Release()

	// Tombstone at 5 starts in first chunk
	// Tombstone at 15 starts in second chunk (index 5 relative to second chunk)
	ts := query.NewBitset()
	ts.Set(5)
	ts.Set(15)

	records := []arrow.RecordBatch{rec}
	tombstones := map[int]*query.Bitset{0: ts}

	sliced, slicedTs := AdaptivelySliceBatches(records, tombstones, strategy)
	defer func() {
		for _, r := range sliced {
			r.Release()
		}
	}()

	require.Equal(t, 2, len(sliced))
	assert.Equal(t, int64(10), sliced[0].NumRows())
	assert.Equal(t, int64(20), sliced[1].NumRows())

	// Check TS for first chunk (index 0)
	ts0 := slicedTs[0]
	assert.True(t, ts0.Contains(5))
	assert.False(t, ts0.Contains(15))

	// Check TS for second chunk (index 1)
	ts1 := slicedTs[1]
	assert.True(t, ts1.Contains(15-10)) // Should be shifted by 10 (value 5)
	// Note: 5 happens to be the same index as the first tombstone in chunk 0,
	// but here it represents original index 15. The previous assertion covers it.
}
