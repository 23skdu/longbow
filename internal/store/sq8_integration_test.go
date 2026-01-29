package store

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQ8_Integration(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	count := 1000

	vecs := make([][]float32, count)
	for i := 0; i < count; i++ {
		v := make([]float32, dims)
		for j := range v {
			v[j] = rand.Float32() * 10
		}
		vecs[i] = v
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < count; i++ {
		listB.Append(true)
		valB.AppendValues(vecs[i], nil)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}

	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 100
	config.SQ8Enabled = true
	config.Dims = dims

	h := NewArrowHNSW(ds, &config)

	// Add batch
	rowIdxs := make([]int, count)
	batchIdxs := make([]int, count)
	for i := 0; i < count; i++ {
		rowIdxs[i] = i
	}
	_, err := h.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)

	// Search
	query := vecs[0]
	// Search(ctx, query, k, filter)
	results, err := h.Search(context.Background(), query, 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// Check top result is self (approximate 0 distance)
	assert.Equal(t, uint32(0), uint32(results[0].ID))
	// SQ8 has loss, but with self it should be very close to 0 if quantized identically
	// or small enough.
	assert.Less(t, math.Abs(float64(results[0].Dist)), 1.0)
}

func TestSQ8_Accuracy(t *testing.T) {
	// Stub accuracy test
}
