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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_PQ_Integration(t *testing.T) {
	// 1. Setup
	dim := 128
	count := 1000
	numCenters := 5
	rng := rand.New(rand.NewSource(12345))

	// Generate some clustered vectors for better retrieval testing
	centers := make([][]float32, numCenters)
	for i := 0; i < numCenters; i++ {
		centers[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			centers[i][d] = rng.Float32() * 10
		}
	}

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
		centerIdx := i % numCenters
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = centers[centerIdx][d] + rng.Float32()
		}
		valB.AppendValues(vec, nil)
		trainingData[i] = vec
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	dataset := &MockDataset{Name: "test_pq", Schema: schema, Records: []arrow.RecordBatch{rec}}

	// 2. Setup PQ Encoder
	// 128 dims -> 16 sub-vectors of 8 dims each. 256 centroids per sub-vector.
	encoder, err := pq.NewPQEncoder(dim, 16, 256)
	require.NoError(t, err)

	err = encoder.Train(trainingData)
	require.NoError(t, err)

	dataset.PQEncoder = encoder

	// 3. Create ArrowHNSW with PQ enabled
	config := types.DefaultArrowHNSWConfig()
	config.PQEnabled = true
	config.M = 16
	config.EfConstruction = 100

	index := NewArrowHNSW(dataset, &config, nil)
	index.SetPQEncoder(encoder)

	// 4. Insert Vectors
	for i := 0; i < count; i++ {
		_, err := index.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// 5. Verify storage
	data := index.data.Load()
	require.NotNil(t, data.VectorsPQ, "VectorsPQ should be allocated")

	pqVec0 := data.GetVectorPQ(0)
	require.NotNil(t, pqVec0)
	assert.Equal(t, 16, len(pqVec0), "PQ code length should match encoder CodeSize")

	// 6. Perform Search
	// Search for something close to center 0
	queryVec := make([]float32, dim)
	for d := 0; d < dim; d++ {
		queryVec[d] = centers[0][d] + 0.5
	}

	results, err := index.SearchVectors(context.Background(), queryVec, 10, nil, types.SearchOptions{})
	require.NoError(t, err)
	require.Len(t, results, 10)

	// Verify results belong mostly to Cluster 0
	cluster0Hits := 0
	for _, res := range results {
		if (uint32(res.ID) % uint32(numCenters)) == 0 {
			cluster0Hits++
		}
	}

	// With PQ and 5 clusters, we should expect good accuracy
	t.Logf("Cluster 0 hits: %d/10", cluster0Hits)
	assert.GreaterOrEqual(t, cluster0Hits, 7, "Search should return mostly vectors from the correct cluster")
}
