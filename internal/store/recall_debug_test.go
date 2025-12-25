package store_test

import (
	"math"
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/hnsw2"
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
	
	rec := builder.NewRecord()
	defer rec.Release()
	
	// Create dataset
	ds := store.NewDataset("debug_test", schema)
	ds.Records = []arrow.RecordBatch{rec}
	
	// ArrowHNSW manages its own location store
	
	// Build hnsw2 index
	config := hnsw2.DefaultConfig()
	hnsw2Index := hnsw2.NewArrowHNSW(ds, config)
	
	// Insert vectors into hnsw2
	for i := 0; i < numVectors; i++ {
		if _, err := hnsw2Index.AddByLocation(0, i); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}
	
	// Debug: Check graph structure
	t.Logf("\nHNSW2 Graph Structure:")
	t.Logf("  Entry point: %d", hnsw2Index.GetEntryPoint())
	t.Logf("  Max level: %d", hnsw2Index.GetMaxLevel())
	
	// Test with first vector as query
	queryIdx := 0
	query := vectors[queryIdx]
	k := 10
	
	// Verify query is normalized
	var sumSq float32
	for _, v := range query {
		sumSq += v * v
	}
	t.Logf("Query vector norm: %.6f (should be ~1.0)", math.Sqrt(float64(sumSq)))
	
	// Get GROUND TRUTH results using brute-force search
	type idDist struct {
		id   uint32
		dist float32
	}
	allDistances := make([]idDist, numVectors)
	for i := 0; i < numVectors; i++ {
		var distSq float32
		for j := 0; j < dim; j++ {
			diff := query[j] - vectors[i][j]
			distSq += diff * diff
		}
		allDistances[i] = idDist{uint32(i), float32(math.Sqrt(float64(distSq)))}
	}
	
	// Sort by distance (brute force)
	for i := 0; i < len(allDistances); i++ {
		for j := i + 1; j < len(allDistances); j++ {
			if allDistances[j].dist < allDistances[i].dist {
				allDistances[i], allDistances[j] = allDistances[j], allDistances[i]
			}
		}
	}
	
	// Take top k as ground truth
	groundTruth := allDistances[:k]
	
	t.Logf("\nGround truth (brute-force) top-%d results:", k)
	for i, item := range groundTruth {
		t.Logf("  %d. ID=%d, L2=%.6f", i+1, item.id, item.dist)
	}
	
	// Get hnsw2 results with higher ef for better recall
	ef := k * 10 // Use 10x k for better recall
	hnsw2Results, err := hnsw2Index.Search(query, k, ef, nil)
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
		t.Logf("  %d. ID=%d, reported_score=%.6f, L2=%.6f, L2²=%.6f", 
			i+1, result.ID, result.Score, dist, distSq)
	}
	
	// Check overlap with ground truth
	groundTruthSet := make(map[uint32]bool)
	for _, item := range groundTruth {
		groundTruthSet[item.id] = true
	}
	
	matches := 0
	for _, result := range hnsw2Results {
		if groundTruthSet[uint32(result.ID)] {
			matches++
		}
	}
	
	recall := float64(matches) / float64(len(groundTruth))
	t.Logf("\nRecall: %d/%d = %.2f%%", matches, len(groundTruth), recall*100)
	
	if recall < 0.90 {
		t.Errorf("Recall too low: %.2f%%, expected >= 90%%", recall*100)
	}
}
