package store

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_BinaryQuantization_Integration(t *testing.T) {
	// Setup
	dim := 128
	count := 1000
	rand.Seed(time.Now().UnixNano())

	// Create Config with BQ Enabled
	config := DefaultArrowHNSWConfig()
	config.BQEnabled = true
	config.M = 16
	config.EfConstruction = 100

	// Create Dataset
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	// Generate Vectors: 5 clusters
	// Cluster 0: all +ve
	// Cluster 1: all -ve
	// Cluster 2: alternating +/-
	// etc.
	for i := 0; i < count; i++ {
		listB.Append(true)
		vec := make([]float32, dim)

		cluster := i % 5
		for d := 0; d < dim; d++ {
			noise := rand.Float32()*0.2 - 0.1
			switch cluster {
			case 0:
				vec[d] = 1.0 + noise
			case 1:
				vec[d] = -1.0 + noise
			case 2:
				if d%2 == 0 {
					vec[d] = 1.0 + noise
				} else {
					vec[d] = -1.0 + noise
				}
			default:
				vec[d] = rand.Float32()*2 - 1
			}
		}
		valB.AppendValues(vec, nil)
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	dataset := &Dataset{Schema: schema, Records: []arrow.RecordBatch{rec}}

	// Create Index
	index := NewArrowHNSW(dataset, config, nil)

	// Insert Vectors
	for i := 0; i < count; i++ {
		_, err := index.AddByLocation(0, i)
		require.NoError(t, err)
	}

	// Verify Data Structure
	// Check if BQ vectors are stored
	data := index.data.Load()
	require.NotNil(t, data.VectorsBQ, "VectorsBQ should be allocated")

	// Check content of BQ vector for Cluster 0 (record 0)
	// Expect all 1s (approx) -> 0xFF...
	bqVec0 := data.GetVectorBQ(0)
	require.NotNil(t, bqVec0)
	// With 128 dims, we have 2 uint64s.
	// 0: All 1s = 0xFFFFFFFFFFFFFFFF
	// 1: All 1s = 0xFFFFFFFFFFFFFFFF
	// Allow some noise to flip a bit? No, +1.0 + noise(+-0.1) is always > 0.
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), bqVec0[0])
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), bqVec0[1])

	// Perform Search
	// Search for something close to Cluster 0
	queryVec := make([]float32, dim)
	for d := 0; d < dim; d++ {
		queryVec[d] = 0.9 // Positive
	}

	// Standard Search
	results, err := index.SearchVectors(queryVec, 10, nil)
	require.NoError(t, err)
	require.Len(t, results, 10)

	// Verify results are from Cluster 0 (IDs mostly multiple of 5)
	for _, res := range results {
		// Just check that they are somewhat relevant?
		// Cluster 0 IDs are 0, 5, 10...
		isCluster0 := (res.ID % 5) == 0
		// In high dims with BQ, it should find these.
		assert.True(t, isCluster0, "Result ID %d should belong to Cluster 0 (mod 5 == 0)", res.ID)

		// Distance should be Hamming distance.
		// Cluster 0 vs Cluster 0 should be near 0.
		// Result.Score is float32(hamming).
		// We expect 0 or very small.
		assert.Less(t, res.Score, float32(5), "Distance should be small for exact match cluster")
	}

	t.Logf("BQ Integration Test Passed with %d records", count)
}
