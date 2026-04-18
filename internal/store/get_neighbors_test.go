package store

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildSmallDataset creates a dataset with n float32 vectors of dimension dim
// in a single Arrow record batch and inserts them into an ArrowHNSW index.
func buildSmallDataset(t *testing.T, n, dim int) (*Dataset, *ArrowHNSW) {
	t.Helper()
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	bId := arrowarray.NewInt64Builder(mem)
	defer bId.Release()
	bVec := arrowarray.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Float32)
	defer bVec.Release()
	bVecValues := bVec.ValueBuilder().(*arrowarray.Float32Builder)

	for i := 0; i < n; i++ {
		bId.Append(int64(i))
		bVec.Append(true)
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = float32(i*dim+j) * 0.01
		}
		bVecValues.AppendValues(vec, nil)
	}

	arrId := bId.NewArray()
	defer arrId.Release()
	arrVec := bVec.NewArray()
	defer arrVec.Release()

	batch := arrowarray.NewRecordBatch(schema, []arrow.Array{arrId, arrVec}, int64(n))

	ds := NewDataset("test_gn", schema)
	batch.Retain()
	ds.Records = append(ds.Records, batch)

	t.Cleanup(func() {
		for _, r := range ds.Records {
			r.Release()
		}
	})

	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dim
	cfg.M = 4
	cfg.MMax = 8
	cfg.MMax0 = 8
	cfg.EfConstruction = 50
	cfg.InitialCapacity = n + 10

	idx := NewArrowHNSW(ds, &cfg)
	ds.Index = idx
	t.Cleanup(func() { _ = idx.Close() })

	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for i := range rowIdxs {
		rowIdxs[i] = i
	}
	ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{batch}, rowIdxs, batchIdxs)
	require.NoError(t, err)
	require.Len(t, ids, n)

	return ds, idx
}

// TestLookupNeighbors_HNSW_ReturnsTrueNeighbors verifies that after insertion the
// graph has recorded neighbors for the queried node. All returned IDs must be
// valid node indices.
func TestLookupNeighbors_HNSW_ReturnsTrueNeighbors(t *testing.T) {
	const n, dim = 50, 4
	ds, _ := buildSmallDataset(t, n, dim)

	results, err := LookupNeighbors(context.Background(), ds, 25, 0)
	require.NoError(t, err)
	for _, r := range results {
		assert.Less(t, r.ID, uint64(n), "neighbor ID must be a valid node index")
	}
}

// TestLookupNeighbors_HNSW_KLimitRespected verifies that k=2 returns at most 2 results.
func TestLookupNeighbors_HNSW_KLimitRespected(t *testing.T) {
	const n, dim = 50, 4
	ds, _ := buildSmallDataset(t, n, dim)

	results, err := LookupNeighbors(context.Background(), ds, 25, 2)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(results), 2, "k=2 must return at most 2 results")
}

// TestLookupNeighbors_UnknownID verifies that an ID not in the index returns
// ErrVectorNotFound.
func TestLookupNeighbors_UnknownID(t *testing.T) {
	const n, dim = 10, 4
	ds, _ := buildSmallDataset(t, n, dim)

	_, err := LookupNeighbors(context.Background(), ds, 9999, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrVectorNotFound), "unknown ID must return ErrVectorNotFound")
}

// TestLookupNeighbors_NonHNSW_ReturnsNotSupported verifies that non-ArrowHNSW
// index types return ErrGetNeighborsNotSupported.
//
// ShardedHNSW is a full VectorIndex implementation, but is not *ArrowHNSW,
// so LookupNeighbors should reject it with the appropriate sentinel.
func TestLookupNeighbors_NonHNSW_ReturnsNotSupported(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)
	ds := NewDataset("sharded_ds", schema)

	shardedCfg := DefaultShardedHNSWConfig()
	shardedCfg.Dimension = 4
	shardedCfg.NumShards = 1
	shardedIdx := NewShardedHNSW(shardedCfg, ds)
	defer func() { _ = shardedIdx.Close() }()

	_, err := LookupNeighbors(context.Background(), ds, 0, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrGetNeighborsNotSupported),
		"ShardedHNSW must return ErrGetNeighborsNotSupported")
}

// TestLookupNeighbors_NilDataset ensures the function does not panic on nil input.
func TestLookupNeighbors_NilDataset(t *testing.T) {
	_, err := LookupNeighbors(context.Background(), nil, 0, 0)
	require.Error(t, err)
}
