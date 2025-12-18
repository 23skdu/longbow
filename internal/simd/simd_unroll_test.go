package simd

import (
"math"
"testing"
)

// =============================================================================
// TDD Tests for Unrolled SIMD Functions
// =============================================================================

// TestEuclideanUnrolledCorrectness verifies unrolled matches scalar
func TestEuclideanUnrolledCorrectness(t *testing.T) {
testCases := []struct {
name string
a    []float32
b    []float32
}{
{"empty", []float32{}, []float32{}},
{"single", []float32{1.0}, []float32{2.0}},
{"two", []float32{1.0, 2.0}, []float32{3.0, 4.0}},
{"three", []float32{1.0, 2.0, 3.0}, []float32{4.0, 5.0, 6.0}},
{"four_exact", []float32{1.0, 2.0, 3.0, 4.0}, []float32{5.0, 6.0, 7.0, 8.0}},
{"five", []float32{1, 2, 3, 4, 5}, []float32{6, 7, 8, 9, 10}},
{"eight_exact", []float32{1, 2, 3, 4, 5, 6, 7, 8}, []float32{9, 10, 11, 12, 13, 14, 15, 16}},
{"nine", []float32{1, 2, 3, 4, 5, 6, 7, 8, 9}, []float32{10, 11, 12, 13, 14, 15, 16, 17, 18}},
{"sixteen", make16(), make16Offset()},
{"128_dim", make128(), make128Offset()},
{"zeros", []float32{0, 0, 0, 0}, []float32{0, 0, 0, 0}},
{"negative", []float32{-1, -2, -3, -4}, []float32{1, 2, 3, 4}},
}

for _, tc := range testCases {
t.Run(tc.name, func(t *testing.T) {
expected := euclideanGeneric(tc.a, tc.b)
got := euclideanUnrolled4x(tc.a, tc.b)
if !almostEqual(expected, got) {
t.Errorf("euclideanUnrolled4x mismatch: expected %v, got %v", expected, got)
}
})
}
}

// TestCosineUnrolledCorrectness verifies unrolled matches scalar
func TestCosineUnrolledCorrectness(t *testing.T) {
testCases := []struct {
name string
a    []float32
b    []float32
}{
{"empty", []float32{}, []float32{}},
{"single", []float32{1.0}, []float32{2.0}},
{"four_exact", []float32{1.0, 2.0, 3.0, 4.0}, []float32{5.0, 6.0, 7.0, 8.0}},
{"five", []float32{1, 2, 3, 4, 5}, []float32{6, 7, 8, 9, 10}},
{"eight_exact", []float32{1, 2, 3, 4, 5, 6, 7, 8}, []float32{9, 10, 11, 12, 13, 14, 15, 16}},
{"128_dim", make128(), make128Offset()},
{"orthogonal", []float32{1, 0, 0, 0}, []float32{0, 1, 0, 0}},
{"parallel", []float32{1, 2, 3, 4}, []float32{2, 4, 6, 8}},
{"antiparallel", []float32{1, 2, 3, 4}, []float32{-1, -2, -3, -4}},
}

for _, tc := range testCases {
t.Run(tc.name, func(t *testing.T) {
expected := cosineGeneric(tc.a, tc.b)
got := cosineUnrolled4x(tc.a, tc.b)
if !almostEqual(expected, got) {
t.Errorf("cosineUnrolled4x mismatch: expected %v, got %v", expected, got)
}
})
}
}

// TestDotUnrolledCorrectness verifies unrolled matches scalar
func TestDotUnrolledCorrectness(t *testing.T) {
testCases := []struct {
name string
a    []float32
b    []float32
}{
{"empty", []float32{}, []float32{}},
{"single", []float32{3.0}, []float32{4.0}},
{"four_exact", []float32{1.0, 2.0, 3.0, 4.0}, []float32{5.0, 6.0, 7.0, 8.0}},
{"five", []float32{1, 2, 3, 4, 5}, []float32{1, 1, 1, 1, 1}},
{"eight_exact", []float32{1, 2, 3, 4, 5, 6, 7, 8}, []float32{1, 1, 1, 1, 1, 1, 1, 1}},
{"128_dim", make128(), make128Ones()},
{"zeros", []float32{0, 0, 0, 0}, []float32{1, 2, 3, 4}},
{"negative", []float32{-1, -2, -3, -4}, []float32{1, 2, 3, 4}},
}

for _, tc := range testCases {
t.Run(tc.name, func(t *testing.T) {
expected := dotGeneric(tc.a, tc.b)
got := dotUnrolled4x(tc.a, tc.b)
if !almostEqual(expected, got) {
t.Errorf("dotUnrolled4x mismatch: expected %v, got %v", expected, got)
}
})
}
}

// TestEuclideanBatchUnrolledCorrectness verifies batch unrolled matches scalar
func TestEuclideanBatchUnrolledCorrectness(t *testing.T) {
query := make128()
vectors := [][]float32{
make128Offset(),
make128(),
make128Ones(),
}

expectedResults := make([]float32, len(vectors))
gotResults := make([]float32, len(vectors))

// Compute expected with scalar
for i, v := range vectors {
expectedResults[i] = euclideanGeneric(query, v)
}

// Compute with unrolled batch
euclideanBatchUnrolled4x(query, vectors, gotResults)

for i := range expectedResults {
if !almostEqual(expectedResults[i], gotResults[i]) {
t.Errorf("batch[%d] mismatch: expected %v, got %v", i, expectedResults[i], gotResults[i])
}
}
}

// =============================================================================
// Benchmarks: Unrolled vs Scalar
// =============================================================================

func BenchmarkEuclidean_Scalar_128dim(b *testing.B) {
a := make128()
vec := make128Offset()
b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = euclideanGeneric(a, vec)
}
}

func BenchmarkEuclidean_Unrolled4x_128dim(b *testing.B) {
a := make128()
vec := make128Offset()
b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = euclideanUnrolled4x(a, vec)
}
}

func BenchmarkCosine_Scalar_128dim(b *testing.B) {
a := make128()
vec := make128Offset()
b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = cosineGeneric(a, vec)
}
}

func BenchmarkCosine_Unrolled4x_128dim(b *testing.B) {
a := make128()
vec := make128Offset()
b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = cosineUnrolled4x(a, vec)
}
}

func BenchmarkDot_Scalar_128dim(b *testing.B) {
a := make128()
vec := make128Offset()
b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = dotGeneric(a, vec)
}
}

func BenchmarkDot_Unrolled4x_128dim(b *testing.B) {
a := make128()
vec := make128Offset()
b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = dotUnrolled4x(a, vec)
}
}

func BenchmarkEuclideanBatch_Scalar_128dim_100vec(b *testing.B) {
query := make128()
vectors := make([][]float32, 100)
for i := range vectors {
vectors[i] = make128Offset()
}
results := make([]float32, 100)
b.ResetTimer()
for i := 0; i < b.N; i++ {
euclideanBatchGeneric(query, vectors, results)
}
}

func BenchmarkEuclideanBatch_Unrolled4x_128dim_100vec(b *testing.B) {
query := make128()
vectors := make([][]float32, 100)
for i := range vectors {
vectors[i] = make128Offset()
}
results := make([]float32, 100)
b.ResetTimer()
for i := 0; i < b.N; i++ {
euclideanBatchUnrolled4x(query, vectors, results)
}
}

// =============================================================================
// Helper functions
// =============================================================================

func almostEqual(a, b float32) bool {
	const epsilon float32 = 1e-4
return float32(math.Abs(float64(a-b))) < epsilon
}

func make16() []float32 {
v := make([]float32, 16)
for i := range v {
v[i] = float32(i + 1)
}
return v
}

func make16Offset() []float32 {
v := make([]float32, 16)
for i := range v {
v[i] = float32(i + 17)
}
return v
}

func make128() []float32 {
v := make([]float32, 128)
for i := range v {
v[i] = float32(i) * 0.1
}
return v
}

func make128Offset() []float32 {
v := make([]float32, 128)
for i := range v {
v[i] = float32(i)*0.1 + 1.0
}
return v
}

func make128Ones() []float32 {
v := make([]float32, 128)
for i := range v {
v[i] = 1.0
}
return v
}
