package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentifyCompactionCandidates(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	createBatch := func(rows int) arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		for i := 0; i < rows; i++ {
			b.Field(0).(*array.Int64Builder).Append(int64(i))
		}
		return b.NewRecordBatch()
	}

	tests := []struct {
		name       string
		batchSizes []int
		targetSize int64
		expected   []CompactionCandidate
	}{
		{
			name:       "all_small_batches",
			batchSizes: []int{10, 10, 10},
			targetSize: 50,
			expected: []CompactionCandidate{
				{StartIdx: 0, EndIdx: 3, TotalRow: 30},
			},
		},
		{
			name:       "one_large_batch_barrier",
			batchSizes: []int{10, 10, 100, 10, 10},
			targetSize: 50,
			expected: []CompactionCandidate{
				{StartIdx: 0, EndIdx: 2, TotalRow: 20},
				{StartIdx: 3, EndIdx: 5, TotalRow: 20},
			},
		},
		{
			name:       "exact_target_match",
			batchSizes: []int{25, 25, 10, 10},
			targetSize: 50,
			expected: []CompactionCandidate{
				{StartIdx: 0, EndIdx: 2, TotalRow: 50},
				{StartIdx: 2, EndIdx: 4, TotalRow: 20},
			},
		},
		{
			name:       "no_candidates",
			batchSizes: []int{100, 100},
			targetSize: 50,
			expected:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batches := make([]arrow.RecordBatch, len(tt.batchSizes))
			for i, s := range tt.batchSizes {
				batches[i] = createBatch(s)
			}
			defer func() {
				for _, b := range batches {
					b.Release()
				}
			}()

			candidates := identifyCompactionCandidates(batches, tt.targetSize)
			assert.Equal(t, tt.expected, candidates)
		})
	}
}

func TestCompactRecords_Basic(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	createBatch := func(startID int, count int) arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		for i := 0; i < count; i++ {
			b.Field(0).(*array.Int64Builder).Append(int64(startID + i))
			b.Field(1).(*array.Float32Builder).Append(float32(startID + i))
		}
		return b.NewRecordBatch()
	}

	// Create 3 small batches
	b1 := createBatch(0, 10)
	b2 := createBatch(10, 10)
	b3 := createBatch(20, 10)
	batches := []arrow.RecordBatch{b1, b2, b3}
	defer b1.Release()
	defer b2.Release()
	defer b3.Release()

	// 1. Test standard merge (no tombstones)
	compacted, remapping, err := compactRecords(mem, schema, batches, nil, 100, "test")
	require.NoError(t, err)
	assert.Len(t, compacted, 1)
	assert.Equal(t, int64(30), compacted[0].NumRows())
	assert.Len(t, remapping, 3)

	// Verify IDs in compacted batch
	ids := compacted[0].Column(0).(*array.Int64)
	for i := 0; i < 30; i++ {
		assert.Equal(t, int64(i), ids.Value(i))
	}
	compacted[0].Release()

	// 2. Test with tombstones
	tombstones := make(map[int]*Bitset)
	tomb1 := NewBitset()
	tomb1.Set(0) // Delete ID 0
	tomb1.Set(5) // Delete ID 5
	tombstones[0] = tomb1

	tomb3 := NewBitset()
	tomb3.Set(9) // Delete ID 29 (last row of b3)
	tombstones[2] = tomb3

	compacted, remapping, err = compactRecords(mem, schema, batches, tombstones, 100, "test")
	require.NoError(t, err)
	assert.Len(t, compacted, 1)
	assert.Equal(t, int64(27), compacted[0].NumRows()) // 30 - 3 = 27

	// Verify Remapping
	// b1 row 0 -> -1
	assert.Equal(t, -1, remapping[0].NewRowIdxs[0])
	// b1 row 1 -> 0
	assert.Equal(t, 0, remapping[0].NewRowIdxs[1])
	// b3 row 9 -> -1
	assert.Equal(t, -1, remapping[2].NewRowIdxs[9])

	compacted[0].Release()
}

func TestFilterTombstones(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	b := array.NewRecordBuilder(mem, schema)
	for i := 0; i < 10; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	defer b.Release()

	tomb := NewBitset()
	tomb.Set(2)
	tomb.Set(8)

	filtered, mapping, removed := filterTombstones(mem, schema, rec, tomb)
	defer filtered.Release()

	assert.Equal(t, int64(8), filtered.NumRows())
	assert.Equal(t, int64(2), removed)
	assert.Equal(t, -1, mapping[2])
	assert.Equal(t, -1, mapping[8])
	assert.Equal(t, 2, mapping[3]) // skipped 2
}

func TestAppendValue_Types(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("fixed_size_list", func(t *testing.T) {
		dt := arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32)
		schema := arrow.NewSchema([]arrow.Field{{Name: "vec", Type: dt}}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		lb := b.Field(0).(*array.FixedSizeListBuilder)
		vb := lb.ValueBuilder().(*array.Float32Builder)

		lb.Append(true)
		vb.AppendValues([]float32{1.1, 2.2, 3.3}, nil)
		lb.Append(true)
		vb.AppendValues([]float32{4.4, 5.5, 6.6}, nil)

		rec := b.NewRecordBatch()
		defer rec.Release()

		// Create a new builder to append into
		b2 := array.NewRecordBuilder(mem, schema)
		defer b2.Release()

		appendValue(b2.Field(0), rec.Column(0), 1)

		res := b2.NewRecordBatch()
		defer res.Release()

		assert.Equal(t, int64(1), res.NumRows())
		col := res.Column(0).(*array.FixedSizeList)
		vals := col.ListValues().(*array.Float32)
		assert.Equal(t, float32(4.4), vals.Value(0))
		assert.Equal(t, float32(6.6), vals.Value(2))
	})

	t.Run("timestamp", func(t *testing.T) {
		dt := arrow.FixedWidthTypes.Timestamp_ns
		schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: dt}}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(123456789))

		rec := b.NewRecordBatch()
		defer rec.Release()

		b2 := array.NewRecordBuilder(mem, schema)
		defer b2.Release()
		appendValue(b2.Field(0), rec.Column(0), 0)

		res := b2.NewRecordBatch()
		defer res.Release()
		assert.Equal(t, arrow.Timestamp(123456789), res.Column(0).(*array.Timestamp).Value(0))
	})
}
