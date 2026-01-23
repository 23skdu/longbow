package simd

import (
	"math"
	"testing"
)

// =============================================================================
// TDD Tests for Parallel Sum Reduction with Multiple Accumulators
// =============================================================================

// TestCosineBatchUnrolledCorrectness verifies batch cosine matches scalar
func TestCosineBatchUnrolledCorrectness(t *testing.T) {
	query := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	vectors := [][]float32{
		{8, 7, 6, 5, 4, 3, 2, 1},
		{1, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 1},
		{1, 2, 3, 4, 5, 6, 7, 8}, // identical to query
	}
	results := make([]float32, len(vectors))

	// Compute with batch unrolled
	err := cosineBatchUnrolled4x(query, vectors, results)
	if err != nil {
		t.Errorf("cosineBatchUnrolled4x error: %v", err)
	}

	// Verify against scalar
	for i, v := range vectors {
		expected, err := cosineGeneric(query, v)
		if err != nil {
			t.Errorf("cosineGeneric error: %v", err)
		}
		if math.Abs(float64(results[i]-expected)) > 1e-4 {
			t.Errorf("cosineBatchUnrolled4x[%d] mismatch: expected %v, got %v", i, expected, results[i])
		}
	}
}

// TestDotBatchUnrolledCorrectness verifies batch dot product matches scalar
func TestDotBatchUnrolledCorrectness(t *testing.T) {
	query := []float32{1, 2, 3, 4, 5, 6, 7, 8}
	vectors := [][]float32{
		{8, 7, 6, 5, 4, 3, 2, 1},
		{1, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 1},
		{2, 2, 2, 2, 2, 2, 2, 2},
	}
	results := make([]float32, len(vectors))

	// Compute with batch unrolled
	err := dotBatchUnrolled4x(query, vectors, results)
	if err != nil {
		t.Errorf("dotBatchUnrolled4x error: %v", err)
	}

	// Verify against scalar
	for i, v := range vectors {
		expected, err := dotGeneric(query, v)
		if err != nil {
			t.Errorf("dotGeneric error: %v", err)
		}
		if math.Abs(float64(results[i]-expected)) > 1e-4 {
			t.Errorf("dotBatchUnrolled4x[%d] mismatch: expected %v, got %v", i, expected, results[i])
		}
	}
}

// TestCosineBatchDispatch verifies CosineBatch API exists and works
func TestCosineBatchDispatch(t *testing.T) {
	query := []float32{1, 0, 0, 0}
	vectors := [][]float32{
		{1, 0, 0, 0}, // cosine = 0 (identical)
		{0, 1, 0, 0}, // cosine = 1 (orthogonal)
	}
	results := make([]float32, len(vectors))

	err := CosineDistanceBatch(query, vectors, results)
	if err != nil {
		t.Errorf("CosineDistanceBatch error: %v", err)
	}

	if results[0] > 1e-4 {
		t.Errorf("identical vectors should have cosine distance ~0, got %v", results[0])
	}
	if math.Abs(float64(results[1]-1.0)) > 1e-4 {
		t.Errorf("orthogonal vectors should have cosine distance ~1, got %v", results[1])
	}
}

// TestDotBatchDispatch verifies DotProductBatch API exists and works
func TestDotBatchDispatch(t *testing.T) {
	query := []float32{1, 2, 3, 4}
	vectors := [][]float32{
		{1, 0, 0, 0}, // dot = 1
		{0, 1, 0, 0}, // dot = 2
		{1, 1, 1, 1}, // dot = 10
	}
	results := make([]float32, len(vectors))

	err := DotProductBatch(query, vectors, results)
	if err != nil {
		t.Errorf("DotProductBatch error: %v", err)
	}

	expected := []float32{1, 2, 10}
	for i, exp := range expected {
		if math.Abs(float64(results[i]-exp)) > 1e-4 {
			t.Errorf("DotProductBatch[%d] expected %v, got %v", i, exp, results[i])
		}
	}
}

// TestParallelReductionHighDimension tests with 768-dim vectors (LLM embedding size)
func TestParallelReductionHighDimension(t *testing.T) {
	dim := 768
	query := make([]float32, dim)
	vectors := make([][]float32, 10)
	for i := range query {
		query[i] = float32(i%10) / 10.0
	}
	for j := range vectors {
		vectors[j] = make([]float32, dim)
		for i := range vectors[j] {
			vectors[j][i] = float32((i+j)%10) / 10.0
		}
	}

	// Test all batch operations
	eucResults := make([]float32, len(vectors))
	cosResults := make([]float32, len(vectors))
	dotResults := make([]float32, len(vectors))

	if err := EuclideanDistanceBatch(query, vectors, eucResults); err != nil {
		t.Errorf("EuclideanDistanceBatch error: %v", err)
	}
	if err := CosineDistanceBatch(query, vectors, cosResults); err != nil {
		t.Errorf("CosineDistanceBatch error: %v", err)
	}
	if err := DotProductBatch(query, vectors, dotResults); err != nil {
		t.Errorf("DotProductBatch error: %v", err)
	}

	// Verify against scalar
	for i, v := range vectors {
		expEuc, errE := euclideanGeneric(query, v)
		expCos, errC := cosineGeneric(query, v)
		expDot, errD := dotGeneric(query, v)

		if errE != nil || errC != nil || errD != nil {
			t.Errorf("generic error")
		}

		if math.Abs(float64(eucResults[i]-expEuc)) > 1e-3 {
			t.Errorf("euclidean[%d] mismatch: got %v, want %v", i, eucResults[i], expEuc)
		}
		if math.Abs(float64(cosResults[i]-expCos)) > 1e-3 {
			t.Errorf("cosine[%d] mismatch: got %v, want %v", i, cosResults[i], expCos)
		}
		if math.Abs(float64(dotResults[i]-expDot)) > 1e-3 {
			t.Errorf("dot[%d] mismatch: got %v, want %v", i, dotResults[i], expDot)
		}
	}
}

// TestAccumulatorIndependence verifies 4 accumulators produce correct sums
func TestAccumulatorIndependence(t *testing.T) {
	// Specifically test that 4 independent accumulators combine correctly
	// Pattern: [1,0,0,0, 0,1,0,0, 0,0,1,0, 0,0,0,1, ...] x [1,1,1,1...]
	// Each accumulator should get exactly 1 contribution per 4 elements
	a := make([]float32, 16)
	b := make([]float32, 16)
	for i := range a {
		a[i] = 0
		b[i] = 1
	}
	// Set diagonal pattern
	a[0], a[5], a[10], a[15] = 1, 1, 1, 1

	dot, err := dotUnrolled4x(a, b)
	if err != nil {
		t.Errorf("dotUnrolled4x error: %v", err)
	}
	if math.Abs(float64(dot-4.0)) > 1e-6 {
		t.Errorf("expected dot=4 (one per accumulator), got %v", dot)
	}
}

// =============================================================================
// Benchmarks: Batch vs Single Loop
// =============================================================================

func BenchmarkCosineBatch_Unrolled4x_128dim_100vec(b *testing.B) {
	dim := 128
	nVec := 100
	query := make([]float32, dim)
	vectors := make([][]float32, nVec)
	results := make([]float32, nVec)
	for i := range query {
		query[i] = float32(i)
	}
	for j := range vectors {
		vectors[j] = make([]float32, dim)
		for i := range vectors[j] {
			vectors[j][i] = float32(i + j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cosineBatchUnrolled4x(query, vectors, results)
	}
}

func BenchmarkDotBatch_Unrolled4x_128dim_100vec(b *testing.B) {
	dim := 128
	nVec := 100
	query := make([]float32, dim)
	vectors := make([][]float32, nVec)
	results := make([]float32, nVec)
	for i := range query {
		query[i] = float32(i)
	}
	for j := range vectors {
		vectors[j] = make([]float32, dim)
		for i := range vectors[j] {
			vectors[j][i] = float32(i + j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dotBatchUnrolled4x(query, vectors, results)
	}
}

// Compare batch vs sequential single-vector calls
func BenchmarkCosine_Sequential_128dim_100vec(b *testing.B) {
	dim := 128
	nVec := 100
	query := make([]float32, dim)
	vectors := make([][]float32, nVec)
	for i := range query {
		query[i] = float32(i)
	}
	for j := range vectors {
		vectors[j] = make([]float32, dim)
		for i := range vectors[j] {
			vectors[j][i] = float32(i + j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range vectors {
			_, _ = CosineDistance(query, v)
		}
	}
}

func BenchmarkDot_Sequential_128dim_100vec(b *testing.B) {
	dim := 128
	nVec := 100
	query := make([]float32, dim)
	vectors := make([][]float32, nVec)
	for i := range query {
		query[i] = float32(i)
	}
	for j := range vectors {
		vectors[j] = make([]float32, dim)
		for i := range vectors[j] {
			vectors[j][i] = float32(i + j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range vectors {
			_, _ = DotProduct(query, v)
		}
	}
}
