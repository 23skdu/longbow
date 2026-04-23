package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"context"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_PQ_ParallelSearch(t *testing.T) {
	// Setup
	dim := 128
	count := 1000
	rng := rand.New(rand.NewSource(42))

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	trainingData := make([][]float32, count)
	for i := 0; i < count; i++ {
		listB.Append(true)
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = rng.Float32()
		}
		valB.AppendValues(vec, nil)
		trainingData[i] = vec
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	dataset := &MockDataset{Name: "test_pq_parallel", Schema: schema, Records: []arrow.RecordBatch{rec}}

	encoder, _ := pq.NewPQEncoder(dim, 16, 256)
	_ = encoder.Train(trainingData)
	dataset.PQEncoder = encoder

	config := types.DefaultArrowHNSWConfig()
	config.PQEnabled = true
	config.ParallelSearch.Enabled = true
	config.ParallelSearch.Workers = 4
	config.ParallelSearch.MinChunkSize = 10 // Force parallel with small count

	index := NewArrowHNSW(dataset, &config, nil)
	index.SetPQEncoder(encoder)

	for i := 0; i < count; i++ {
		_, _ = index.AddByLocation(context.Background(), 0, i)
	}

	queryVec := make([]float32, dim)
	for d := 0; d < dim; d++ {
		queryVec[d] = rng.Float32()
	}

	// This should use the parallel path
	results, err := index.SearchVectors(context.Background(), queryVec, 10, nil, types.SearchOptions{})
	require.NoError(t, err)
	require.Len(t, results, 10)
}
