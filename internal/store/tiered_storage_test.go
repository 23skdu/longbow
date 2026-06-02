package store

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	amemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTieredStorage_OffloadAndFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	path := "test_tiered.dvs"
	defer os.Remove(path)

	dim := 128
	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	remote := storage.NewMockRemoteStorage("s3")
	dvs.SetTieredConfig(remote, 10) // 10MB cache

	// 1. Add some vectors
	vectors := make([][]float32, 10)
	for i := 0; i < 10; i++ {
		v := make([]float32, dim)
		v[0] = float32(i)
		vectors[i] = v
	}

	n, err := dvs.BatchAppend(vectors)
	require.NoError(t, err)
	assert.Equal(t, 10, n)

	// 2. Offload block 0
	ctx := context.Background()
	err = dvs.OffloadBlock(ctx, 0)
	require.NoError(t, err)

	// Verify it's in remote
	exists, _ := remote.Exists(ctx, fmt.Sprintf("blocks/%s/%d", path, 0))
	assert.True(t, exists)

	// 3. Fetch vectors (transparently from remote)
	indices := []int{0, 5, 9}
	results, err := dvs.GetBatch(indices)
	require.NoError(t, err)
	require.Equal(t, len(indices), len(results))

	for i, idx := range indices {
		assert.Equal(t, float32(idx), results[i][0])
	}
}

func TestTieredStorage_EnforcePolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	path := "test_policy.dvs"
	defer os.Remove(path)

	dim := 128
	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	remote := storage.NewMockRemoteStorage("s3")
	dvs.SetTieredConfig(remote, 10)

	// Add vector
	v := make([]float32, dim)
	v[0] = 1.23
	_, _ = dvs.BatchAppend([][]float32{v})

	// Enforce policy with 0 age (all blocks qualify)
	ctx := context.Background()
	n, err := dvs.EnforcePolicy(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Verify block 0 is warm
	results, err := dvs.GetBatch([]int{0})
	require.NoError(t, err)
	assert.Equal(t, float32(1.23), results[0][0])
}

func TestDiskVectorStore_Fallback_Float64(t *testing.T) {
	path := "test_fallback_f64.bin"
	defer os.Remove(path)

	pool := amemory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float64)},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	vBuilder := b.Field(0).(*array.FixedSizeListBuilder)
	vValBuilder := vBuilder.ValueBuilder().(*array.Float64Builder)
	idBuilder := b.Field(1).(*array.Int64Builder)

	// Add 3 vectors
	vBuilder.Append(true)
	vValBuilder.AppendValues([]float64{1.0, 2.0, 3.0, 4.0}, nil)
	idBuilder.Append(0)

	vBuilder.Append(true)
	vValBuilder.AppendValues([]float64{5.0, 6.0, 7.0, 8.0}, nil)
	idBuilder.Append(1)

	vBuilder.Append(true)
	vValBuilder.AppendValues([]float64{9.0, 10.0, 11.0, 12.0}, nil)
	idBuilder.Append(2)

	rec := b.NewRecord()
	defer rec.Release()

	dvs, err := NewDiskVectorStore(path, 4)
	require.NoError(t, err)
	defer dvs.Close()

	n, err := dvs.BatchAppendArrow(rec, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	ds := &Dataset{
		Name:      "test_fallback_f64_ds",
		DiskStore: dvs,
	}

	arrowConfig := index.DefaultArrowHNSWConfig()
	arrowConfig.Dims = 4
	arrowConfig.DataType = types.VectorTypeFloat64
	arrowConfig.SharedVectorSpace = false

	hnsw := index.NewArrowHNSW(ds, &arrowConfig, nil)
	require.NotNil(t, hnsw)

	// Simulate eviction of the memory-backed chunk
	gd := hnsw.GetData()
	require.NotNil(t, gd)
	require.NotEmpty(t, gd.VectorsFloat64Offsets)
	gd.VectorsFloat64Offsets[0] = 0

	// Fetch vector transparently from DiskVectorStore
	vecAny, err := hnsw.GetVector(1)
	require.NoError(t, err)
	require.NotNil(t, vecAny)

	vec, ok := vecAny.([]float64)
	require.True(t, ok)
	assert.Equal(t, []float64{5.0, 6.0, 7.0, 8.0}, vec)
}

func TestDiskVectorStore_Fallback_Float32(t *testing.T) {
	path := "test_fallback_f32.bin"
	defer os.Remove(path)

	pool := amemory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	vBuilder := b.Field(0).(*array.FixedSizeListBuilder)
	vValBuilder := vBuilder.ValueBuilder().(*array.Float32Builder)
	idBuilder := b.Field(1).(*array.Int64Builder)

	// Add 3 vectors
	vBuilder.Append(true)
	vValBuilder.AppendValues([]float32{1.0, 2.0, 3.0, 4.0}, nil)
	idBuilder.Append(0)

	vBuilder.Append(true)
	vValBuilder.AppendValues([]float32{5.0, 6.0, 7.0, 8.0}, nil)
	idBuilder.Append(1)

	vBuilder.Append(true)
	vValBuilder.AppendValues([]float32{9.0, 10.0, 11.0, 12.0}, nil)
	idBuilder.Append(2)

	rec := b.NewRecord()
	defer rec.Release()

	dvs, err := NewDiskVectorStore(path, 4)
	require.NoError(t, err)
	defer dvs.Close()

	n, err := dvs.BatchAppendArrow(rec, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	ds := &Dataset{
		Name:      "test_fallback_f32_ds",
		DiskStore: dvs,
	}

	arrowConfig := index.DefaultArrowHNSWConfig()
	arrowConfig.Dims = 4
	arrowConfig.DataType = types.VectorTypeFloat32
	arrowConfig.SharedVectorSpace = false

	hnsw := index.NewArrowHNSW(ds, &arrowConfig, nil)
	require.NotNil(t, hnsw)

	// Simulate eviction of the memory-backed chunk
	gd := hnsw.GetData()
	require.NotNil(t, gd)
	require.NotEmpty(t, gd.VectorsF32)
	gd.VectorsF32[0] = 0

	// Fetch vector transparently from DiskVectorStore
	vecAny, err := hnsw.GetVector(1)
	require.NoError(t, err)
	require.NotNil(t, vecAny)

	vec, ok := vecAny.([]float32)
	require.True(t, ok)
	assert.Equal(t, []float32{5.0, 6.0, 7.0, 8.0}, vec)
}
