package store_test

import (
	"context"
	"math"
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestRecallDebug helps debug why recall is so low
func TestRecallDebug(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Small test: 100 vectors, dim 32
	numVectors := 100
	dim := 32

	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Generate random vectors (use helper from recall_validation_test.go)
	vectors := generateRandomVectors(numVectors, dim)

	// Build Arrow RecordBatch
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint32Builder)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	valueBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(uint32(i))
		vecBuilder.Append(true)
		for _, v := range vectors[i] {
			valueBuilder.Append(v)
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Create dataset
	ds := store.NewDataset("debug_test", schema)
	ds.Records = []arrow.RecordBatch{rec}

	// Build hnsw2 index
	config := store.DefaultArrowHNSWConfig()
	// NewArrowHNSW signature fixed
	hnsw2Index := store.NewArrowHNSW(ds, &config)

	// Insert vectors into hnsw2
	for i := 0; i < numVectors; i++ {
		if _, err := hnsw2Index.AddByLocation(context.Background(), 0, i); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}

	// Test with first vector as query
	queryIdx := 0
	query := vectors[queryIdx]
	k := 10

	// Get hnsw2 results
	// Search signature fixed
	hnsw2Results, err := hnsw2Index.Search(context.Background(), query, k, nil)
	if err != nil {
		t.Fatalf("hnsw2 search failed: %v", err)
	}

	t.Logf("\nhnsw2 top-%d results:", len(hnsw2Results))
	for i, result := range hnsw2Results {
		// Calculate distance manually
		var distSq float32
		for j := 0; j < dim; j++ {
			diff := query[j] - vectors[result.ID][j]
			distSq += diff * diff
		}
		dist := float32(math.Sqrt(float64(distSq)))
		// Use result.Dist
		t.Logf("  %d. ID=%d, reported_score=%.6f, L2=%.6f, L2²=%.6f",
			i+1, result.ID, result.Dist, dist, distSq)
	}
}
