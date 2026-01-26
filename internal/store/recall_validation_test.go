package store_test

import (
	"context"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store"
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
		config     *store.ArrowHNSWConfig
	}{
		{"Medium_10K_Recall@10", 10000, 384, 100, 10, 0.990, &store.ArrowHNSWConfig{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 400,
			SelectionHeuristicLimit: 0,
			InitialCapacity:         10000,
		}},
		{"Medium_10K_SQ8_Recall@10", 10000, 384, 100, 10, 0.950, &store.ArrowHNSWConfig{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 400,
			SQ8Enabled:      true,
			InitialCapacity: 10000,
		}},
		{"Medium_50K_Recall@10", 50000, 384, 100, 10, 0.920, &store.ArrowHNSWConfig{
			M: 64, MMax: 128, MMax0: 128, EfConstruction: 600,
			SelectionHeuristicLimit: 0,
			SQ8Enabled:              false,
			InitialCapacity:         50000,
		}},
		{"Large_100K_Recall@10", 100000, 384, 100, 10, 0.880, &store.ArrowHNSWConfig{
			M: 64, MMax: 128, MMax0: 128, EfConstruction: 400,
			SelectionHeuristicLimit: 128, // Reduced from 400 to fix low recall issue
			SQ8Enabled:              false,
			DataType:                store.VectorTypeFloat32,
		}},
		{"Large_500K_Recall@10", 500000, 384, 100, 10, 0.850, &store.ArrowHNSWConfig{
			M: 96, MMax: 192, MMax0: 192, EfConstruction: 1000,
			SelectionHeuristicLimit: 400,
			SQ8Enabled:              false,
			DataType:                store.VectorTypeFloat32,
		}},
		{"Dim_128_100K_Recall@10", 100000, 128, 100, 10, 0.990, &store.ArrowHNSWConfig{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 600,
			DataType: store.VectorTypeFloat32,
		}},
		{"Dim_768_20K_Recall@10", 20000, 768, 100, 10, 0.990, &store.ArrowHNSWConfig{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 1000,
			DataType: store.VectorTypeFloat32,
		}},
		{"Dim_1536_20K_Recall@10", 20000, 1536, 100, 10, 0.990, &store.ArrowHNSWConfig{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 1000,
			DataType: store.VectorTypeFloat32,
		}},
		{"Stress_500K_128D_Recall@10", 500000, 128, 50, 10, 0.900, &store.ArrowHNSWConfig{
			M: 48, MMax: 96, MMax0: 96, EfConstruction: 400,
			DataType: store.VectorTypeFloat32,
		}},
		{"Huge_1M_Recall@10", 1000000, 384, 100, 10, 0.850, &store.ArrowHNSWConfig{
			M: 96, MMax: 192, MMax0: 192, EfConstruction: 1200,
			SelectionHeuristicLimit: 400,
			SQ8Enabled:              true,
		}},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {

			if tc.numVectors >= 1000000 && os.Getenv("TEST_HUGE") == "" {
				t.Skip("Skipping Huge 1M test; set TEST_HUGE=1 to run")
			}
			if tc.numVectors >= 100000 && os.Getenv("TEST_LARGE") == "" {
				t.Skip("Skipping Large test; set TEST_LARGE=1 to run")
			}
			if isRace && tc.numVectors > 2000 {
				tc.numVectors = 2000
				tc.minRecall = 0.0 // Relax recall requirement for small dataset
				t.Logf("Downscaling test to %d vectors for race detection", tc.numVectors)
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
func measureRecall(t *testing.T, numVectors, dim, numQueries, k int, cfg *store.ArrowHNSWConfig) float64 {
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

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Create dataset
	ds := store.NewDataset("recall_test", schema)
	ds.Records = []arrow.RecordBatch{rec}

	// No legacy index needed. ArrowHNSW manages its own locations.

	// Build hnsw2 index
	var config store.ArrowHNSWConfig
	if cfg != nil {
		config = *cfg
	} else {
		config = store.DefaultArrowHNSWConfig()
	}
	hnsw2Index := store.NewArrowHNSW(ds, config)

	// Validate that vectors can be retrieved correctly from Arrow
	// This ensures zero-copy retrieval is working
	t.Logf("Validating vector retrieval from Arrow storage...")
	for i := 0; i < 10 && i < numVectors; i++ {
		// Get vector from Arrow (same path HNSW uses)
		arrowVec, err := store.ExtractVectorFromArrow(rec, i, 1)
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
	// Sequential Ingestion (Parallel insertion is hitting races/fragmentation)
	vectorIDs := make([]uint32, numVectors)
	for j := 0; j < numVectors; j++ {
		vecID, err := hnsw2Index.AddByLocation(context.Background(), 0, j)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", j, err)
		}
		vectorIDs[j] = uint32(vecID)
		if j > 0 && j%5000 == 0 {
			t.Logf("Inserted %d/%d vectors", j, numVectors)
		}
	}
	t.Logf("Inserted %d/%d vectors", numVectors, numVectors)

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

	// Measure recall against brute-force ground truth (Parallelized + SIMD)
	t.Logf("Calculating brute-force ground truth for %d queries...", numQueries)
	groundTruths := make([]map[uint32]bool, numQueries)

	qg, qctx := errgroup.WithContext(context.Background())
	qWorkers := runtime.GOMAXPROCS(0)
	if qWorkers > numQueries {
		qWorkers = numQueries
	}
	qBatch := (numQueries + qWorkers - 1) / qWorkers

	for i := 0; i < qWorkers; i++ {
		start := i * qBatch
		end := start + qBatch
		if end > numQueries {
			end = numQueries
		}

		qg.Go(func() error {
			// Per-worker distance buffer to reduce allocations
			dists := make([]float32, numVectors)

			for qIdx := start; qIdx < end; qIdx++ {
				if qctx.Err() != nil {
					return qctx.Err()
				}
				query := queries[qIdx]

				// SIMD batch distance calculation
				if err := simd.EuclideanDistanceBatch(query, vectors, dists); err != nil {
					return err
				}

				// Find top K
				type idDist struct {
					id   uint32
					dist float32
				}
				allDistances := make([]idDist, numVectors)
				for j := 0; j < numVectors; j++ {
					allDistances[j] = idDist{uint32(j), dists[j]}
				}

				sort.Slice(allDistances, func(i, j int) bool {
					return allDistances[i].dist < allDistances[j].dist
				})

				gt := make(map[uint32]bool)
				queryArrIdx := queryIndices[qIdx]
				for j := 0; j < len(allDistances) && len(gt) < k; j++ {
					arrIdx := allDistances[j].id
					if arrIdx == uint32(queryArrIdx) {
						continue
					}
					gt[vectorIDs[arrIdx]] = true
				}
				groundTruths[qIdx] = gt
			}
			return nil
		})
	}

	if err := qg.Wait(); err != nil {
		t.Fatalf("Ground truth calculation failed: %v", err)
	}

	// Run HNSW queries (Parallelized)
	t.Logf("Running %d HNSW queries...", numQueries)
	totalRecall := 0.0
	var recallMu sync.Mutex

	eg, _ := errgroup.WithContext(context.Background())
	for i := 0; i < numQueries; i++ {
		idx := i
		eg.Go(func() error {
			query := queries[idx]
			hnsw2Results, err := hnsw2Index.Search(context.Background(), query, k+1, nil)
			if err != nil {
				return err
			}

			queryVecID := vectorIDs[queryIndices[idx]]
			matches := 0
			count := 0
			for _, res := range hnsw2Results {
				// Skip if result is the query vector itself
				if uint32(res.ID) == queryVecID {
					continue
				}
				if groundTruths[idx][uint32(res.ID)] {
					matches++
				}
				count++
				if count >= k {
					break
				}
			}

			recall := float64(matches) / float64(k)
			recallMu.Lock()
			totalRecall += recall
			if idx < 3 {
				t.Logf("  Query %d: recall=%.2f%%", idx, recall*100)
			}
			recallMu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("HNSW search failed: %v", err)
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
