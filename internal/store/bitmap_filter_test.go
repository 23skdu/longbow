package store

import (
	"context"
	"testing"

	qry "github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateFilterBitset(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("test", schema)
	idx := NewHNSWIndex(ds)
	ds.Index = idx

	// Create a batch with some data
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	catBuilder := builder.Field(1).(*array.StringBuilder)
	vecBuilder := builder.Field(2).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	ids := []int64{1, 2, 3, 4, 5}
	cats := []string{"tech", "bio", "tech", "finance", "tech"}

	idBuilder.AppendValues(ids, nil)
	catBuilder.AppendValues(cats, nil)

	for i := 0; i < 5; i++ {
		vecBuilder.Append(true)
		vec := make([]float32, 128)
		vec[0] = float32(i)
		valBuilder.AppendValues(vec, nil)
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	ds.Records = append(ds.Records, rec)

	// Add to index to populate location store
	for i := 0; i < 5; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// 1. Test Filter: category == "tech" (should match 0, 2, 4)
	filters := []qry.Filter{
		{Field: "category", Operator: "eq", Value: "tech"},
	}

	bitset, err := ds.GenerateFilterBitset(filters)
	require.NoError(t, err)
	require.NotNil(t, bitset)
	defer bitset.Release()

	assert.Equal(t, uint64(3), bitset.Count())

	// Check VectorIDs. HNSWIndex assigned them starting from 0 (monotonic)
	// Bitset sets bits for VectorIDs
	for i := 0; i < 5; i++ {
		vid, _ := idx.GetVectorID(Location{BatchIdx: 0, RowIdx: i})
		if cats[i] == "tech" {
			assert.True(t, bitset.Contains(int(vid)), "expected bitset to contain vid for RowIdx %d", i)
		} else {
			assert.False(t, bitset.Contains(int(vid)), "expected bitset to NOT contain vid for RowIdx %d", i)
		}
	}

	// 2. Test Cache
	bitset2, err := ds.GenerateFilterBitset(filters)
	require.NoError(t, err)
	require.NotNil(t, bitset2)
	assert.Equal(t, bitset.Count(), bitset2.Count())
	bitset2.Release()
}

func TestSearchWithBitmapFiltering(t *testing.T) {
	// Setup same dataset
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("test_search", schema)
	idx := NewHNSWIndex(ds)
	ds.Index = idx

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	catBuilder := builder.Field(1).(*array.StringBuilder)
	vecBuilder := builder.Field(2).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	catBuilder.AppendValues([]string{"A", "B", "A"}, nil)

	// Vectors: [1,0], [0,1], [1,1]
	vecs := [][]float32{{1, 0}, {0, 1}, {1, 1}}
	for _, v := range vecs {
		vecBuilder.Append(true)
		valBuilder.AppendValues(v, nil)
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	ds.Records = append(ds.Records, rec)

	for i := range vecs {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Search for [1,0], k=3, filter category="B"
	// Should only return RowIdx 1
	queryVec := []float32{1, 0}
	filters := []qry.Filter{
		{Field: "category", Operator: "eq", Value: "B"},
	}

	results, err := idx.SearchVectors(context.Background(), queryVec, 3, filters, SearchOptions{})
	require.NoError(t, err)
	require.Len(t, results, 1)

	loc, ok := idx.GetLocation(uint32(results[0].ID))
	require.True(t, ok)
	l := loc.(Location)
	assert.Equal(t, 0, l.BatchIdx)
	assert.Equal(t, 1, l.RowIdx)
}

func TestShardedSearchWithBitmapFiltering(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	ds := NewDataset("test_sharded_search", schema)

	shardedConfig := DefaultShardedHNSWConfig()
	shardedConfig.NumShards = 2
	shardedConfig.Dimension = 2
	idx := NewShardedHNSW(shardedConfig, ds)
	ds.Index = idx

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	catBuilder := builder.Field(1).(*array.StringBuilder)
	vecBuilder := builder.Field(2).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.AppendValues([]int64{1, 2, 3, 4}, nil)
	catBuilder.AppendValues([]string{"A", "B", "A", "B"}, nil)

	vecs := [][]float32{{1, 0}, {0, 1}, {1, 1}, {0, 0}}
	for _, v := range vecs {
		vecBuilder.Append(true)
		valBuilder.AppendValues(v, nil)
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	ds.Records = append(ds.Records, rec)

	for i := range vecs {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Search for [0.1, 0.9], k=4, filter category="B"
	// Should return RowIdx 1 and 3
	queryVec := []float32{0.1, 0.9}
	filters := []qry.Filter{
		{Field: "category", Operator: "eq", Value: "B"},
	}

	results, err := idx.SearchVectors(context.Background(), queryVec, 4, filters, SearchOptions{})
	require.NoError(t, err)
	require.Len(t, results, 2)

	// Scores: [0.1, 0.9] vs [0, 1] (Row 1) = dist; [0.1, 0.9] vs [0,0] (Row 3) = dist
	// Merged results should be sorted by score
	assert.Equal(t, uint32(1), uint32(results[0].ID))
	assert.Equal(t, uint32(3), uint32(results[1].ID))
}
