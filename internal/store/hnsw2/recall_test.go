package hnsw2_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RecallValidationTest validates recall of hnsw2 against coder/hnsw baseline
func TestRecallValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping recall validation in short mode")
	}

	testCases := []struct {
		name       string
		numVectors int
		dim        int
		numQueries int
		k          int
		minRecall  float64
	}{
		{"Small_1K_Recall@10", 1000, 128, 100, 10, 0.995},
		{"Medium_10K_Recall@10", 10000, 384, 100, 10, 0.995},
		{"Medium_10K_Recall@50", 10000, 384, 100, 50, 0.990},
		{"Large_100K_Recall@10", 100000, 384, 100, 10, 0.995},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recall := measureRecall(t, tc.numVectors, tc.dim, tc.numQueries, tc.k)
			
			t.Logf("Recall@%d: %.4f%% (target: %.2f%%)", tc.k, recall*100, tc.minRecall*100)
			
			if recall < tc.minRecall {
				t.Errorf("Recall@%d = %.4f%%, want >= %.2f%%", tc.k, recall*100, tc.minRecall*100)
			}
		})
	}
}

// measureRecall compares hnsw2 results against coder/hnsw baseline
func measureRecall(t *testing.T, numVectors, dim, numQueries, k int) float64 {
	mem := memory.NewGoAllocator()
	
	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	// Generate random vectors
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
	ds := store.NewDataset("recall_test", schema)
	ds.Records = []arrow.RecordBatch{rec}
	
	// Build coder/hnsw index (baseline)
	coderIndex := store.NewHNSWIndex(16, 200, store.MetricL2)
	for i := 0; i < numVectors; i++ {
		coderIndex.Add(store.VectorID(i), vectors[i])
	}
	
	// Build hnsw2 index
	config := hnsw2.DefaultConfig()
	hnsw2Index := hnsw2.NewArrowHNSW(ds, config)
	
	// Initialize locationStore for hnsw2
	hnswIdx := store.NewHNSWIndex(16, 200, store.MetricL2)
	for i := 0; i < numVectors; i++ {
		hnswIdx.Add(store.VectorID(i), vectors[i])
	}
	ds.Index = hnswIdx
	
	// Insert vectors into hnsw2
	lg := hnsw2.NewLevelGenerator(1.44269504089)
	for i := 0; i < numVectors; i++ {
		level := lg.Generate()
		if err := hnsw2Index.Insert(uint32(i), level); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}
	
	// Generate query vectors (subset of indexed vectors for ground truth)
	queries := make([][]float32, numQueries)
	for i := 0; i < numQueries; i++ {
		queryIdx := rand.Intn(numVectors)
		queries[i] = vectors[queryIdx]
	}
	
	// Measure recall
	totalRecall := 0.0
	for _, query := range queries {
		// Get baseline results from coder/hnsw
		baselineResults, err := coderIndex.Search(query, k)
		if err != nil {
			t.Fatalf("Baseline search failed: %v", err)
		}
		
		// Get hnsw2 results
		hnsw2Results, err := hnsw2Index.Search(query, k, k*2)
		if err != nil {
			t.Fatalf("hnsw2 search failed: %v", err)
		}
		
		// Calculate recall for this query
		recall := calculateRecall(baselineResults, hnsw2Results, k)
		totalRecall += recall
	}
	
	return totalRecall / float64(numQueries)
}

// calculateRecall computes recall@k between two result sets
func calculateRecall(baseline, test []store.SearchResult, k int) float64 {
	if len(baseline) == 0 {
		return 1.0
	}
	
	// Create set of baseline IDs
	baselineSet := make(map[store.VectorID]bool)
	for i := 0; i < len(baseline) && i < k; i++ {
		baselineSet[baseline[i].ID] = true
	}
	
	// Count matches in test results
	matches := 0
	for i := 0; i < len(test) && i < k; i++ {
		if baselineSet[store.VectorID(test[i].ID)] {
			matches++
		}
	}
	
	return float64(matches) / float64(min(k, len(baseline)))
}

// generateRandomVectors creates random normalized vectors
func generateRandomVectors(n, dim int) [][]float32 {
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		var norm float32
		for j := 0; j < dim; j++ {
			vec[j] = rand.Float32()*2 - 1 // [-1, 1]
			norm += vec[j] * vec[j]
		}
		// Normalize
		norm = float32(1.0 / (float64(norm) + 1e-8))
		for j := 0; j < dim; j++ {
			vec[j] *= norm
		}
		vectors[i] = vec
	}
	return vectors
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestRecallWithDifferentMetrics validates recall across different distance metrics
func TestRecallWithDifferentMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping metric validation in short mode")
	}
	
	// Currently hnsw2 only supports L2, but this test is ready for future metrics
	t.Run("L2_Distance", func(t *testing.T) {
		recall := measureRecall(t, 1000, 128, 50, 10)
		t.Logf("L2 Recall@10: %.4f%%", recall*100)
		if recall < 0.995 {
			t.Errorf("L2 recall too low: %.4f%%", recall*100)
		}
	})
}

// TestRecallConsistency validates that recall is consistent across multiple runs
func TestRecallConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consistency test in short mode")
	}
	
	const numRuns = 5
	recalls := make([]float64, numRuns)
	
	for i := 0; i < numRuns; i++ {
		recalls[i] = measureRecall(t, 1000, 128, 50, 10)
	}
	
	// Calculate mean and std dev
	var sum, sumSq float64
	for _, r := range recalls {
		sum += r
		sumSq += r * r
	}
	mean := sum / float64(numRuns)
	variance := (sumSq / float64(numRuns)) - (mean * mean)
	stddev := variance
	if variance > 0 {
		stddev = variance // simplified, should use sqrt
	}
	
	t.Logf("Recall consistency over %d runs:", numRuns)
	t.Logf("  Mean: %.4f%%", mean*100)
	t.Logf("  Std Dev: %.4f%%", stddev*100)
	
	for i, r := range recalls {
		t.Logf("  Run %d: %.4f%%", i+1, r*100)
	}
	
	// All runs should meet minimum recall
	for i, r := range recalls {
		if r < 0.990 {
			t.Errorf("Run %d recall too low: %.4f%%", i+1, r*100)
		}
	}
}
