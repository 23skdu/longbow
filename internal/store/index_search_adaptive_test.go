package store

import (
	"testing"

	"context"
	"math/rand"
	"time"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdaptiveSearch_RetryLogic tests that search expands its limit when initial candidates are filtered out
func TestAdaptiveSearch_RetryLogic(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Schema: vector (fixed_size_list<2>), id_col (int64)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		{Name: "id_col", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create dataset with 1000 vectors
	count := 1000
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	idB := builder.Field(1).(*array.Int64Builder)

	// Vector i is [i, i], id_col is i
	for i := 0; i < count; i++ {
		listB.Append(true)
		valB.AppendValues([]float32{float32(i), float32(i)}, nil)
		idB.Append(int64(i))
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	dataset := &Dataset{
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}

	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 100
	config.InitialCapacity = 1024

	index := NewArrowHNSW(dataset, config)

	// Insert all vectors
	for i := 0; i < count; i++ {
		_, err := index.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Query for vector [0, 0]. Nearest neighbors are 0, 1, 2, ...
	q := []float32{0, 0}
	k := 5

	// Filter: id_col == 95
	// This provides very high selectivity (1/1000).
	// Initial limit 50 (k*10). 95 is not in 0-49 neighbors.
	// Retry limit 250. 95 SHOULD be found (neighbors are 0,1,2...). 95 is 95th neighbor.

	filters := []query.Filter{
		{Field: "id_col", Operator: "=", Value: "95"},
	}

	results, err := index.SearchVectors(context.Background(), q, k, filters, SearchOptions{})
	require.NoError(t, err)

	// Verify we got 1 result
	assert.Equal(t, 1, len(results), "Should find 1 result after retry")

	// Verify results obey filter
	for _, res := range results {
		id := int(res.ID)
		assert.Equal(t, 95, id, "Result ID should be 95")
	}
}

func TestAdaptiveHNSW_AdjustsM(t *testing.T) {
	// 1. Setup Config with AdaptiveM enabled
	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.MMax = 32
	cfg.AdaptiveMEnabled = true
	cfg.AdaptiveMThreshold = 100
	cfg.Dims = 128

	// 2. Create Index
	idx := NewArrowHNSW(nil, cfg)

	// 3. Generate structured data (high intrinsic dimensionality)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	dims := 128
	n := 150
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		vecs[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vecs[i][j] = rng.Float32()
		}
	}

	// 4. Insert vectors
	for i := 0; i < n; i++ {
		err := idx.InsertWithVector(uint32(i+1), vecs[i], 0)
		require.NoError(t, err)
	}

	// 5. Assert M has changed
	// We access the config directly from the struct (internal test)
	currentM := idx.config.M
	t.Logf("Initial M: %d, Current M: %d", 16, currentM)

	// With 128D uniform noise, intrinsic dimensionality is high.
	// Adaptive strategy should INCREASE M to maintain connectivity/recall.
	assert.Greater(t, currentM, 16, "M should have increased due to high dimensionality")
}
