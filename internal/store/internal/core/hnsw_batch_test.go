package core_test

import (
	"github.com/23skdu/longbow/internal/store/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Test Helpers
// =============================================================================

// createTestMockDataset creates a test core.MockDataset with random vectors
func createTestMockDataset(t *testing.T, name string, dims, numVectors int) *core.MockDataset {
	t.Helper()
	mem := memory.NewGoAllocator()

	// Generate deterministic test vectors
	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
		for j := range vectors[i] {
			vectors[i][j] = float32((i*dims+j)%100) / 100.0
		}
	}

	rec := core.MakeBatchTestRecord(mem, dims, vectors)

	return &core.MockDataset{
		Name:    name,
		Records: []arrow.RecordBatch{rec},
	}
}

// =============================================================================
// HNSW Batch Search Integration Tests
// =============================================================================

func TestHNSWIndex_SearchBatch(t *testing.T) {
	ds := createTestMockDataset(t, "batch-search-test", 16, 100)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	idx := core.NewTestHNSWIndex(ds)
	for i := 0; i < 100; i++ {
		_, _ = idx.AddByLocation(context.Background(), 0, i)
	}

	queries := [][]float32{
		makeTestVector(16, 0),
		makeTestVector(16, 1),
		makeTestVector(16, 2),
	}

	results := idx.SearchBatch(queries, 5)

	if len(results) != len(queries) {
		t.Fatalf("Expected %d result sets, got %d", len(queries), len(results))
	}

	for qi, res := range results {
		if len(res) == 0 {
			t.Errorf("Query %d returned no results", qi)
		}
		if len(res) > 5 {
			t.Errorf("Query %d returned %d results, expected max 5", qi, len(res))
		}
	}
}

func TestHNSWIndex_RerankBatch(t *testing.T) {
	ds := createTestMockDataset(t, "rerank-batch-test", 16, 50)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	idx := core.NewTestHNSWIndex(ds)
	for i := 0; i < 50; i++ {
		_, _ = idx.AddByLocation(context.Background(), 0, i)
	}

	query := makeTestVector(16, 0)
	candidateIDs := []types.VectorID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	reranked := idx.RerankBatch(query, candidateIDs, 3)

	if len(reranked) != 3 {
		t.Fatalf("Expected 3 reranked results, got %d", len(reranked))
	}

	prevDist := float32(-1.0)
	for i, r := range reranked {
		if r.Distance < prevDist {
			t.Errorf("Results not sorted at index %d: %f < %f", i, r.Distance, prevDist)
		}
		prevDist = r.Distance
	}
}

func TestHNSWIndex_SearchBatchWithArena(t *testing.T) {
	ds := createTestMockDataset(t, "batch-arena-test", 16, 100)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	idx := core.NewTestHNSWIndex(ds)
	for i := 0; i < 100; i++ {
		_, _ = idx.AddByLocation(context.Background(), 0, i)
	}

	arena := core.NewSearchArena(64 * 1024)

	queries := [][]float32{
		makeTestVector(16, 0),
		makeTestVector(16, 1),
	}

	results := idx.SearchBatchWithArena(queries, 5, arena)

	if len(results) != 2 {
		t.Fatalf("Expected 2 result sets, got %d", len(results))
	}

	if arena.Offset() == 0 {
		t.Error("Arena offset is 0, expected allocations")
	}
}

func TestHNSWIndex_SearchBatch_Empty(t *testing.T) {
	ds := createTestMockDataset(t, "empty-batch-test", 16, 10)
	idx := core.NewTestHNSWIndex(ds)

	// Empty queries
	results := idx.SearchBatch([][]float32{}, 5)
	if results != nil {
		t.Error("Expected nil for empty queries")
	}

	// k <= 0
	results = idx.SearchBatch([][]float32{{1.0}}, 0)
	if results != nil {
		t.Error("Expected nil for k=0")
	}
}

func TestHNSWIndex_RerankBatch_Empty(t *testing.T) {
	ds := createTestMockDataset(t, "rerank-empty-test", 16, 10)
	idx := core.NewTestHNSWIndex(ds)

	// Empty candidates
	results := idx.RerankBatch([]float32{1.0}, []types.VectorID{}, 5)
	if results != nil {
		t.Error("Expected nil for empty candidates")
	}

	// k <= 0
	results = idx.RerankBatch([]float32{1.0}, []types.VectorID{0, 1}, 0)
	if results != nil {
		t.Error("Expected nil for k=0")
	}
}

// =============================================================================
// core.RankedResult type tests
// =============================================================================

func TestRankedResult_Fields(t *testing.T) {
	r := core.RankedResult{
		ID:       types.VectorID(42),
		Distance: 1.5,
	}

	if r.ID != 42 {
		t.Errorf("Expected ID 42, got %d", r.ID)
	}
	if r.Distance != 1.5 {
		t.Errorf("Expected Distance 1.5, got %f", r.Distance)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkSequential_1000Candidates(b *testing.B) {
	query := make([]float32, 128)
	for i := range query {
		query[i] = float32(i) / 128.0
	}

	candidates := make([][]float32, 1000)
	for i := range candidates {
		candidates[i] = make([]float32, 128)
		for j := range candidates[i] {
			candidates[i][j] = float32(i+j) / 1000.0
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results := make([]float32, len(candidates))
		for j, cand := range candidates {
			results[j], _ = simd.EuclideanDistance(query, cand)
		}
	}
}

// =============================================================================
// Helper functions
// =============================================================================

func makeTestVector(dims, seed int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32((seed*dims+i)%100) / 100.0
	}
	return v
}
