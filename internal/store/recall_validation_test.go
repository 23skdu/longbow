package store_test

import (
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestRecallValidation validates recall of hnsw2 against coder/hnsw baseline
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
	coderIndex := store.NewHNSWIndex(ds)
	for i := 0; i < numVectors; i++ {
		coderIndex.Add(0, i) // BatchIdx=0, RowIdx=i
	}
	ds.Index = coderIndex
	
	// Build hnsw2 index
	config := hnsw2.DefaultConfig()
	hnsw2Index := hnsw2.NewArrowHNSW(ds, config)
	
	// Insert vectors into hnsw2
	lg := hnsw2.NewLevelGenerator(1.44269504089)
	for i := 0; i < numVectors; i++ {
		level := lg.Generate()
		if err := hnsw2Index.Insert(uint32(i), level); err != nil {
			t.Fatalf("Failed to insert vector %d: %v", i, err)
		}
	}
	
	// Generate query vectors (random subset for testing)
	queries := make([][]float32, numQueries)
	queryIndices := make([]int, numQueries)
	for i := 0; i < numQueries; i++ {
		queryIdx := rand.Intn(numVectors)
		queryIndices[i] = queryIdx
		queries[i] = vectors[queryIdx]
	}
	
	// Measure recall
	totalRecall := 0.0
	for i, query := range queries {
		// Get baseline results from coder/hnsw
		baselineIDs, err := coderIndex.Search(query, k)
		if err != nil {
			t.Fatalf("Baseline search failed: %v", err)
		}
		
		// Get hnsw2 results
		hnsw2Results, err := hnsw2Index.Search(query, k, k*2)
		if err != nil {
			t.Fatalf("hnsw2 search failed: %v", err)
		}
		
		// Calculate recall for this query
		recall := calculateRecall(baselineIDs, hnsw2Results, k)
		totalRecall += recall
		
		// Log first few queries for debugging
		if i < 3 {
			t.Logf("  Query %d (vector %d): recall=%.2f%%, baseline=%d results, hnsw2=%d results",
				i, queryIndices[i], recall*100, len(baselineIDs), len(hnsw2Results))
		}
	}
	
	return totalRecall / float64(numQueries)
}

// calculateRecall computes recall@k between baseline and test results
func calculateRecall(baselineIDs []store.VectorID, testResults []store.SearchResult, k int) float64 {
	if len(baselineIDs) == 0 {
		return 1.0
	}
	
	// Create set of baseline IDs
	baselineSet := make(map[store.VectorID]bool)
	for i := 0; i < len(baselineIDs) && i < k; i++ {
		baselineSet[baselineIDs[i]] = true
	}
	
	// Count matches in test results
	matches := 0
	for i := 0; i < len(testResults) && i < k; i++ {
		if baselineSet[store.VectorID(testResults[i].ID)] {
			matches++
		}
	}
	
	return float64(matches) / float64(min(k, len(baselineIDs)))
}

// generateRandomVectors creates random normalized vectors
func generateRandomVectors(n, dim int) [][]float32 {
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		var sumSq float32
		for j := 0; j < dim; j++ {
			vec[j] = rand.Float32()*2 - 1 // [-1, 1]
			sumSq += vec[j] * vec[j]
		}
		// Normalize
		if sumSq > 0 {
			norm := float32(1.0) / float32(sumSq)
			for j := 0; j < dim; j++ {
				vec[j] *= norm
			}
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

// TestRecallConsistency validates that recall is consistent across multiple runs
func TestRecallConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consistency test in short mode")
	}
	
	const numRuns = 3
	recalls := make([]float64, numRuns)
	
	for i := 0; i < numRuns; i++ {
		recalls[i] = measureRecall(t, 500, 128, 50, 10)
	}
	
	// Calculate mean
	var sum float64
	for _, r := range recalls {
		sum += r
	}
	mean := sum / float64(numRuns)
	
	t.Logf("Recall consistency over %d runs:", numRuns)
	t.Logf("  Mean: %.4f%%", mean*100)
	
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
