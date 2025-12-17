package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Test Helpers
// =============================================================================

// createTestDataset creates a test Dataset with random vectors
func createTestDataset(t *testing.T, name string, dims, numVectors int) *Dataset {
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

	rec := makeBatchTestRecord(mem, dims, vectors)

	return &Dataset{
		Name:    name,
		Records: []arrow.RecordBatch{rec},
		mu:      sync.RWMutex{},
	}
}

// makeBatchTestRecord creates an Arrow RecordBatch for testing
func makeBatchTestRecord(mem memory.Allocator, dims int, vectors [][]float32) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	for i, vec := range vectors {
		idBuilder.Append(int64(i))
		listBuilder.Append(true)
		for _, v := range vec {
			vecBuilder.Append(v)
		}
	}

	return array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, int64(len(vectors)))
}

// =============================================================================
// TDD Tests for BatchDistanceCalculator
// =============================================================================

func TestBatchDistanceCalculator_Creation(t *testing.T) {
	calc := NewBatchDistanceCalculator(128)
	if calc == nil {
		t.Fatal("NewBatchDistanceCalculator returned nil")
	}
	if calc.Dims() != 128 {
		t.Errorf("Expected dims 128, got %d", calc.Dims())
	}
}

func TestBatchDistanceCalculator_SingleQuery(t *testing.T) {
	calc := NewBatchDistanceCalculator(4)

	query := []float32{1.0, 0.0, 0.0, 0.0}
	candidates := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.0, 0.0, 1.0, 0.0},
		{2.0, 0.0, 0.0, 0.0},
	}

	distances := calc.ComputeDistances(query, candidates)

	if len(distances) != len(candidates) {
		t.Fatalf("Expected %d distances, got %d", len(candidates), len(distances))
	}

	for i, cand := range candidates {
		expected := simd.EuclideanDistance(query, cand)
		if !floatClose(distances[i], expected, 1e-6) {
			t.Errorf("Distance[%d]: expected %f, got %f", i, expected, distances[i])
		}
	}
}

func TestBatchDistanceCalculator_MultiQuery(t *testing.T) {
	calc := NewBatchDistanceCalculator(4)

	queries := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
	}

	candidates := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.5, 0.5, 0.0, 0.0},
	}

	distanceMatrix := calc.ComputeDistanceMatrix(queries, candidates)

	if len(distanceMatrix) != len(queries) {
		t.Fatalf("Expected %d query results, got %d", len(queries), len(distanceMatrix))
	}

	for qi, query := range queries {
		for ci, cand := range candidates {
			expected := simd.EuclideanDistance(query, cand)
			if !floatClose(distanceMatrix[qi][ci], expected, 1e-6) {
				t.Errorf("Distance[%d][%d]: expected %f, got %f", qi, ci, expected, distanceMatrix[qi][ci])
			}
		}
	}
}

func TestBatchDistanceCalculator_ReuseBuffer(t *testing.T) {
	calc := NewBatchDistanceCalculator(4)

	query := []float32{1.0, 0.0, 0.0, 0.0}
	candidates := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
	}

	buffer := make([]float32, len(candidates))
	calc.ComputeDistancesInto(query, candidates, buffer)

	for i, cand := range candidates {
		expected := simd.EuclideanDistance(query, cand)
		if !floatClose(buffer[i], expected, 1e-6) {
			t.Errorf("Buffer[%d]: expected %f, got %f", i, expected, buffer[i])
		}
	}
}

func TestBatchDistanceCalculator_EmptyCandidates(t *testing.T) {
	calc := NewBatchDistanceCalculator(4)

	query := []float32{1.0, 0.0, 0.0, 0.0}
	candidates := [][]float32{}

	distances := calc.ComputeDistances(query, candidates)

	if len(distances) != 0 {
		t.Errorf("Expected empty distances, got %d", len(distances))
	}
}

func TestBatchDistanceCalculator_LargeBatch(t *testing.T) {
	calc := NewBatchDistanceCalculator(128)

	query := make([]float32, 128)
	for i := range query {
		query[i] = float32(i) / 128.0
	}

	candidates := make([][]float32, 1000)
	for i := range candidates {
		candidates[i] = make([]float32, 128)
		for j := range candidates[i] {
			candidates[i][j] = float32(i*j) / 128000.0
		}
	}

	distances := calc.ComputeDistances(query, candidates)

	if len(distances) != 1000 {
		t.Fatalf("Expected 1000 distances, got %d", len(distances))
	}

	for _, idx := range []int{0, 100, 500, 999} {
		expected := simd.EuclideanDistance(query, candidates[idx])
		if !floatClose(distances[idx], expected, 1e-5) {
			t.Errorf("Distance[%d]: expected %f, got %f", idx, expected, distances[idx])
		}
	}
}

// =============================================================================
// HNSW Batch Search Integration Tests
// =============================================================================

func TestHNSWIndex_SearchBatch(t *testing.T) {
	ds := createTestDataset(t, "batch-search-test", 16, 100)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	idx := NewHNSWIndex(ds)
	for i := 0; i < 100; i++ {
		_ = idx.Add(0, i)
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
	ds := createTestDataset(t, "rerank-batch-test", 16, 50)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	idx := NewHNSWIndex(ds)
	for i := 0; i < 50; i++ {
		_ = idx.Add(0, i)
	}

	query := makeTestVector(16, 0)
	candidateIDs := []VectorID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

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
	ds := createTestDataset(t, "batch-arena-test", 16, 100)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	idx := NewHNSWIndex(ds)
	for i := 0; i < 100; i++ {
		_ = idx.Add(0, i)
	}

	arena := NewSearchArena(64 * 1024)

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
	ds := createTestDataset(t, "empty-batch-test", 16, 10)
	idx := NewHNSWIndex(ds)

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
	ds := createTestDataset(t, "rerank-empty-test", 16, 10)
	idx := NewHNSWIndex(ds)

	// Empty candidates
	results := idx.RerankBatch([]float32{1.0}, []VectorID{}, 5)
	if results != nil {
		t.Error("Expected nil for empty candidates")
	}

	// k <= 0
	results = idx.RerankBatch([]float32{1.0}, []VectorID{0, 1}, 0)
	if results != nil {
		t.Error("Expected nil for k=0")
	}
}

// =============================================================================
// RankedResult type tests
// =============================================================================

func TestRankedResult_Fields(t *testing.T) {
	r := RankedResult{
		ID:       VectorID(42),
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

func BenchmarkBatchDistanceCalculator_1000Candidates(b *testing.B) {
	calc := NewBatchDistanceCalculator(128)

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
		_ = calc.ComputeDistances(query, candidates)
	}
}

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
			results[j] = simd.EuclideanDistance(query, cand)
		}
	}
}

// =============================================================================
// Helper functions
// =============================================================================

func floatClose(a, b, tolerance float32) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

func makeTestVector(dims, seed int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32((seed*dims+i)%100) / 100.0
	}
	return v
}

// TestBatchDistanceCalculator_VariousDims tests different vector dimensions
func TestBatchDistanceCalculator_VariousDims(t *testing.T) {
	for _, dims := range []int{4, 32, 64, 128, 256} {
		t.Run(fmt.Sprintf("dims_%d", dims), func(t *testing.T) {
			ds := createTestDataset(t, "various-dims-test", dims, 10)
			if ds == nil {
				t.Skip("Could not create test dataset")
			}

			calc := NewBatchDistanceCalculator(dims)
			query := makeTestVector(dims, 0)
			candidates := [][]float32{
				makeTestVector(dims, 1),
				makeTestVector(dims, 2),
			}

			distances := calc.ComputeDistances(query, candidates)
			if len(distances) != 2 {
				t.Errorf("Expected 2 distances, got %d", len(distances))
			}
		})
	}
}
