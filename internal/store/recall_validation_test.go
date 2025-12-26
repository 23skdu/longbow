package store_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
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
		config     *hnsw2.Config
	}{
		{"Medium_10K_Recall@10", 10000, 384, 100, 10, 0.995, &hnsw2.Config{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 400,
			SelectionHeuristicLimit: 0,
			Ml: 1.0 / math.Log(48),
			Alpha: 1.0,
			KeepPrunedConnections: true,
		}},
		{"Medium_50K_Recall@10", 50000, 384, 100, 10, 0.920, &hnsw2.Config{
			M: 80, MMax: 160, MMax0: 160, EfConstruction: 500,
			SelectionHeuristicLimit: 0,
			Ml: 1.0 / math.Log(80),
			Alpha: 1.0,
			KeepPrunedConnections: false,
			SQ8Enabled:            false,
			RefinementFactor:      1.0,
		}},
		{"Large_100K_Recall@10", 100000, 384, 100, 10, 0.995, &hnsw2.Config{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 400,
			SelectionHeuristicLimit: 0,
			Ml: 1.0 / math.Log(48),
			Alpha: 1.0,
			KeepPrunedConnections: true,
			SQ8Enabled:            false,
			RefinementFactor:      1.0,
		}},
		{"Large_500K_Recall@10", 500000, 384, 100, 10, 0.990, &hnsw2.Config{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 400,
			SelectionHeuristicLimit: 0,
			Ml: 1.0 / math.Log(48),
			Alpha: 1.0,
			KeepPrunedConnections: true,
			SQ8Enabled:            false,
			RefinementFactor:      1.0,
		}},
		{"Dim_128_100K_Recall@10", 100000, 128, 100, 10, 0.990, &hnsw2.Config{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 800,
			Ml: 1.0 / math.Log(48),
			Alpha: 1.0,
		}},
		{"Dim_768_20K_Recall@10", 20000, 768, 100, 10, 0.990, &hnsw2.Config{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 800,
			Ml: 1.0 / math.Log(48),
			Alpha: 1.0,
		}},
		{"Dim_1536_20K_Recall@10", 20000, 1536, 100, 10, 0.990, &hnsw2.Config{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 800,
			Ml: 1.0 / math.Log(48),
			Alpha: 1.0,
		}},
		{"Stress_500K_128D_Recall@10", 500000, 128, 50, 10, 0.900, &hnsw2.Config{
			M: 32, MMax: 64, MMax0: 64, EfConstruction: 200, // Reduced Ef for speed
			Ml: 1.0 / math.Log(32),
			Alpha: 1.0,
		}},
		{"Huge_1M_Recall@10", 1000000, 384, 100, 10, 0.900, &hnsw2.Config{
			M: 32, MMax: 64, MMax0: 48, EfConstruction: 200,
			Ml: 1.0 / math.Log(32),
			Alpha: 1.0,
			KeepPrunedConnections: true, // Key for high recall
			SQ8Enabled:            true,
			RefinementFactor:      3.0,
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.numVectors >= 1000000 && os.Getenv("TEST_HUGE") == "" {
				t.Skip("Skipping Huge 1M test; set TEST_HUGE=1 to run")
			}

			recall := measureRecall(t, tc.numVectors, tc.dim, tc.numQueries, tc.k, tc.config)
			
			t.Logf("Recall@%d: %.4f%% (target: %.2f%%)", tc.k, recall*100, tc.minRecall*100)
			
			if recall < tc.minRecall {
				t.Errorf("Recall@%d = %.4f%%, want >= %.2f%%", tc.k, recall*100, tc.minRecall*100)
			}
		})
	}
}

// measureRecall compares hnsw2 results against brute-force ground truth
func measureRecall(t *testing.T, numVectors, dim, numQueries, k int, cfg *hnsw2.Config) float64 {
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
	
	// No legacy index needed. ArrowHNSW manages its own locations.
	
	// Build hnsw2 index
	var config hnsw2.Config
	if cfg != nil {
		config = *cfg
	} else {
		config = hnsw2.DefaultConfig()
	}
	hnsw2Index := hnsw2.NewArrowHNSW(ds, config)
	
	// Validate that vectors can be retrieved correctly from Arrow
	// This ensures zero-copy retrieval is working
	t.Logf("Validating vector retrieval from Arrow storage...")
	for i := 0; i < 10 && i < numVectors; i++ {
		// Get vector from Arrow (same path HNSW uses)
		arrowVec, err := hnsw2.ExtractVectorFromArrow(rec, i, 1)
		if err != nil {
			t.Fatalf("Failed to get vector %d from Arrow: %v", i, err)
		}
		
		// Compare with original
		if len(arrowVec) != len(vectors[i]) {
			t.Fatalf("Vector %d length mismatch: Arrow=%d, Original=%d", i, len(arrowVec), len(vectors[i]))
		}
		
		for j := 0; j < len(arrowVec); j++ {
			if arrowVec[j] != vectors[i][j] {
				t.Fatalf("Vector %d mismatch at dim %d: Arrow=%.6f, Original=%.6f", 
					i, j, arrowVec[j], vectors[i][j])
			}
		}
	}
	t.Logf("Vector retrieval validation passed!")
	
	// Insert vectors
	t.Logf("Inserting %d vectors concurrently...", numVectors)
	
	// Track VectorID mapping: arrayIdx -> VectorID
	vectorIDs := make([]uint32, numVectors)
	
	g, ctx := errgroup.WithContext(context.Background())
	// Limit concurrency provided by GOMAXPROCS is implicit in goroutine scheduling, 
	// but we launch numVectors goroutines which is too many. 
	// Use a semaphore or batching? 
	// Previous logic was creating a goroutine PER VECTOR. That is 50,000 goroutines.
	// That works in Go but adds overhead.
	// Previous successful concurrent logic used worker pool or just many goroutines.
	// Let's use runtime.NumCPU() workers for cleaner implementation.
	
	numWorkers := runtime.NumCPU()
	batchSize := (numVectors + numWorkers - 1) / numWorkers
	
	var progressCtr int32
	
	for i := 0; i < numWorkers; i++ {
		startIdx := i * batchSize
		endIdx := startIdx + batchSize
		if endIdx > numVectors {
			endIdx = numVectors
		}
		
		g.Go(func() error {
			// Check context
			if ctx.Err() != nil {
				return ctx.Err()
			}
			
			for j := startIdx; j < endIdx; j++ {
				vecID, err := hnsw2Index.AddByLocation(0, j)
				if err != nil {
					return fmt.Errorf("failed to insert vector %d: %w", j, err)
				}
				// vectorIDs[j] is safe to write if j is unique per worker
				vectorIDs[j] = vecID
				
				// Progress logging (atomic)
				newCtr := atomic.AddInt32(&progressCtr, 1)
				if newCtr%1000 == 0 {
					t.Logf("Inserted %d/%d vectors", newCtr, numVectors)
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Concurrent insertion failed: %v", err)
	}
	
	// Log average degree at Layer 0 for diagnostics
	graphMetrics := hnsw2Index.AnalyzeGraph()
	t.Logf("Graph Metrics: %s", graphMetrics.String())
		
	// Generate query vectors (random subset for testing)
	queries := make([][]float32, numQueries)
	queryIndices := make([]int, numQueries)
	for i := 0; i < numQueries; i++ {
		queryIdx := rand.Intn(numVectors)
		queries[i] = vectors[queryIdx]
		queryIndices[i] = queryIdx
	}
	
	// Measure recall against brute-force ground truth
	totalRecall := 0.0
	for i, query := range queries {
		// Compute ground truth using brute force
		type idDist struct {
			id   uint32
			dist float32
		}
		allDistances := make([]idDist, numVectors)
		for j := 0; j < numVectors; j++ {
			var distSq float32
			for d := 0; d < dim; d++ {
				diff := query[d] - vectors[j][d]
				distSq += diff * diff
			}
			allDistances[j] = idDist{uint32(j), distSq}
		}
		
		// Sort by distance
		sort.Slice(allDistances, func(i, j int) bool {
			return allDistances[i].dist < allDistances[j].dist
		})
		
		// Take top k as ground truth (using VectorIDs, not array indices)
		// Skip the query vector itself (distance 0)
		groundTruth := make(map[uint32]bool)
		queryArrayIdx := queryIndices[i]
		for j := 0; j < len(allDistances) && len(groundTruth) < k; j++ {
			arrayIdx := allDistances[j].id
			if arrayIdx == uint32(queryArrayIdx) {
				continue // Skip query vector itself
			}
			vecID := vectorIDs[arrayIdx]
			groundTruth[vecID] = true
		}
		
		// Get hnsw2 results with higher ef for better recall
		// Request k+1 results because query is likely in the graph and will be returned
		hnsw2Results, err := hnsw2Index.Search(query, k+1, k*200, nil)
		if err != nil {
			t.Fatalf("hnsw2 search failed: %v", err)
		}
		
		// Filter out the query vector itself from HNSW results
		queryVecID := vectorIDs[queryIndices[i]]
		var filteredResults []store.SearchResult
		for _, res := range hnsw2Results {
			if uint32(res.ID) == queryVecID {
				continue
			}
			filteredResults = append(filteredResults, res)
		}
		
		// Trim to top k
		if len(filteredResults) > k {
			filteredResults = filteredResults[:k]
		}
		hnsw2Results = filteredResults
		
		// Calculate recall for this query
		matches := 0
		for _, result := range hnsw2Results {
			if groundTruth[uint32(result.ID)] {
				matches++
			}
		}
		
		recall := float64(matches) / float64(k)
		totalRecall += recall
		
		// Log first few queries for debugging
		if i < 3 {
			t.Logf("  Query %d: recall=%.2f%%, hnsw2=%d results",
				i, recall*100, len(hnsw2Results))
		}
	}
	
	return totalRecall / float64(numQueries)
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
			norm := float32(1.0) / float32(math.Sqrt(float64(sumSq)))
			for j := 0; j < dim; j++ {
				vec[j] *= norm
			}
		}
		vectors[i] = vec
	}
	return vectors
}



// TestRecallConsistency validates that recall is consistent across multiple runs
func TestRecallConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consistency test in short mode")
	}
	
	const numRuns = 3
	recalls := make([]float64, numRuns)
	
	for i := 0; i < numRuns; i++ {
		recalls[i] = measureRecall(t, 500, 128, 50, 10, nil)
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
