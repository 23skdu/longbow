package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestCompactRecords_Incremental tests the incremental compaction logic in isolation
func TestCompactRecords_Incremental(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int64}},
		nil,
	)

	// Helper to create a batch with N rows
	createBatch := func(rows int) arrow.RecordBatch {
		b := array.NewRecordBuilder(pool, schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues(make([]int64, rows), nil)
		return b.NewRecordBatch()
	}

	b1 := createBatch(100)
	b2 := createBatch(100)
	b3 := createBatch(1000)
	b4 := createBatch(50)
	b5 := createBatch(50)
	defer func() {
		b1.Release()
		b2.Release()
		b3.Release()
		b4.Release()
		b5.Release()
	}()

	records := []arrow.RecordBatch{b1, b2, b3, b4, b5}
	target := int64(300)

	compacted, remapping, err := compactRecords(pool, schema, records, nil, target, "test")
	require.NoError(t, err)

	require.Len(t, compacted, 3)
	require.Equal(t, int64(200), compacted[0].NumRows())
	require.Equal(t, int64(1000), compacted[1].NumRows())
	require.Equal(t, int64(100), compacted[2].NumRows())

	require.Equal(t, 0, remapping[0].NewBatchIdx)
	// remapping[0].NewRowIdxs should be 0..99
	require.Equal(t, 0, remapping[0].NewRowIdxs[0])

	require.Equal(t, 0, remapping[1].NewBatchIdx)
	require.Equal(t, 100, remapping[1].NewRowIdxs[0])

	require.Equal(t, 1, remapping[2].NewBatchIdx)
	require.Equal(t, 0, remapping[2].NewRowIdxs[0])

	require.Equal(t, 2, remapping[3].NewBatchIdx)
	require.Equal(t, 0, remapping[3].NewRowIdxs[0])

	require.Equal(t, 2, remapping[4].NewBatchIdx)
	require.Equal(t, 50, remapping[4].NewRowIdxs[0])
}

// TestCompaction_IndexIntegrity validates HNSW index updates
func TestCompaction_IndexIntegrity(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, time.Hour)
	defer func() { _ = s.Close() }()

	// 1. Create Dataset Manually
	vectorDim := 4
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "text", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(vectorDim), arrow.PrimitiveTypes.Float32)},
		}, nil,
	)
	ds := NewDataset("test_compaction", schema)
	ds.Index = NewHNSWIndex(ds)

	s.mu.Lock()
	s.datasets["test_compaction"] = ds
	s.mu.Unlock()

	// 2. Add Vectors in small batches
	for i := 0; i < 10; i++ {
		b := array.NewRecordBuilder(s.mem, schema)

		bldr0 := b.Field(0).(*array.StringBuilder)
		vecB := b.Field(1).(*array.FixedSizeListBuilder)
		floatB := vecB.ValueBuilder().(*array.Float32Builder)

		for j := 0; j < 10; j++ {
			bldr0.Append(fmt.Sprintf("batch%d-row%d", i, j))
			vecB.Append(true)
			vec := make([]float32, vectorDim)
			vec[0] = float32(i*10 + j)
			floatB.AppendValues(vec, nil)
		}
		rec := b.NewRecordBatch()

		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		ds.BatchNodes = append(ds.BatchNodes, -1) // Unspecified node
		batchIdx := len(ds.Records) - 1
		ds.dataMu.Unlock()

		// Add to HNSW
		hnswIdx, ok := ds.Index.(*HNSWIndex)
		require.True(t, ok)

		for rowIdx := 0; rowIdx < int(rec.NumRows()); rowIdx++ {
			_, err := hnswIdx.AddSafe(rec, rowIdx, batchIdx)
			require.NoError(t, err)
		}
	}

	require.Equal(t, 100, ds.IndexLen())
	require.Len(t, ds.Records, 10)

	// 3. Verify Search BEFORE Compaction
	query := []float32{55, 0, 0, 0}
	res, _ := ds.SearchDataset(query, 1)
	require.Len(t, res, 1)

	// Verify result via Location Lookup
	loc, found := ds.Index.GetLocation(res[0].ID)
	require.True(t, found)
	// Retrieve value from record
	rec := ds.Records[loc.BatchIdx]
	col := rec.Column(0).(*array.String)
	val := col.Value(loc.RowIdx)
	require.Equal(t, "batch5-row5", val)

	// 4. Trigger Compaction
	s.compactionConfig.TargetBatchSize = 50
	err := s.CompactDataset("test_compaction")
	require.NoError(t, err)

	require.Len(t, ds.Records, 2)
	require.Equal(t, int64(50), ds.Records[0].NumRows())
	require.Equal(t, int64(50), ds.Records[1].NumRows())

	// 5. Verify Search AFTER Compaction
	resAfter, _ := ds.SearchDataset(query, 1)
	require.Len(t, resAfter, 1)

	locAfter, foundAfter := ds.Index.GetLocation(resAfter[0].ID)
	require.True(t, foundAfter)
	recAfter := ds.Records[locAfter.BatchIdx]
	colAfter := recAfter.Column(0).(*array.String)
	valAfter := colAfter.Value(locAfter.RowIdx)
	require.Equal(t, "batch5-row5", valAfter)
}

// TestCompaction_Tombstones verifies deletion filtering during compaction
func TestCompaction_Tombstones(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, time.Hour)
	defer func() { _ = s.Close() }()

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	ds := NewDataset("tombstone_test", schema)
	s.mu.Lock()
	s.datasets["tombstone_test"] = ds
	s.mu.Unlock()

	// Add 4 batches of 10
	for i := 0; i < 4; i++ {
		b := array.NewRecordBuilder(s.mem, schema)
		b.Field(0).(*array.Int64Builder).AppendValues(make([]int64, 10), nil)
		rec := b.NewRecordBatch()
		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		ds.BatchNodes = append(ds.BatchNodes, -1)
		ds.dataMu.Unlock()
	}

	// Delete from batch 0, row 5
	if ds.Tombstones[0] == nil {
		ds.Tombstones[0] = NewBitset()
	}
	ds.Tombstones[0].Set(5)

	// Delete from batch 2, row 2
	if ds.Tombstones[2] == nil {
		ds.Tombstones[2] = NewBitset()
	}
	ds.Tombstones[2].Set(2)

	// Compact with target 100 (merge all 4 -> 1 batch of 40 theoretically, but with filtering it will be 38)
	s.compactionConfig.TargetBatchSize = 100
	err := s.CompactDataset("tombstone_test")
	require.NoError(t, err)

	require.Len(t, ds.Records, 1)
	require.Equal(t, int64(38), ds.Records[0].NumRows())

	// Tombstones should have been cleared because they are physically removed
	require.Empty(t, ds.Tombstones)
}
