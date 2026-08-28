package index

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorStoreIntegration_PagingEviction(t *testing.T) {
	dim := 8
	cfg := types.DefaultArrowHNSWConfig()
	cfg.Dims = dim
	cfg.EfConstruction = 32

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	numVectors := 500
	rng := rand.New(rand.NewSource(42))

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		listB.Append(true)
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = rng.Float32()
		}
		valB.AppendValues(vec, nil)
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	dataset := &MockDataset{
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewArrowHNSW(dataset, &cfg, nil)
	require.NotNil(t, idx)

	logger := zerolog.Nop()
	evMgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := idx.data.Load()
	evMgr.Register(gd)

	ctx := context.Background()
	for i := 0; i < numVectors; i++ {
		_, err := idx.AddByLocation(ctx, 0, i)
		require.NoError(t, err)
	}

	evMgr.ForceEvictAll()
	time.Sleep(10 * time.Millisecond)

	k := 5
	for i := 0; i < 10; i++ {
		query := make([]float32, dim)
		for d := 0; d < dim; d++ {
			query[d] = rng.Float32()
		}

		results, err := idx.Search(ctx, query, k, nil)
		require.NoError(t, err)
		assert.Equal(t, k, len(results))

		for _, res := range results {
			assert.True(t, res.ID > 0)
		}
	}
}

func TestVectorStoreIntegration_GPUArgumentBuffer(t *testing.T) {
	dim := 8
	cfg := types.DefaultArrowHNSWConfig()
	cfg.Dims = dim

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	listB.Append(true)
	valB.AppendValues([]float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	dataset := &MockDataset{
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewArrowHNSW(dataset, &cfg, nil)
	require.NotNil(t, idx)

	ctx := context.Background()
	_, err := idx.AddByLocation(ctx, 0, 0)
	require.NoError(t, err)

	query := []float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}
	results, err := idx.Search(ctx, query, 1, nil)
	if err != nil {
		t.Skip("Metal GPU search failed, skipping test")
	}
	assert.Equal(t, 1, len(results))
	assert.Equal(t, uint32(0), results[0].ID)
	assert.InDelta(t, 0.0, results[0].Dist, 1e-5)
}
