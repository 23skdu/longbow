//go:build !amd64

package simd

import "math"

// Fallback implementations for non-AMD64 platforms
// These provide reference behavior for testing on ARM/other architectures

// DotProductFMA falls back to scalar implementation on non-AMD64
func DotProductFMA(a, b []float32) float32 {
if len(a) == 0 || len(a) != len(b) {
return 0
}
var sum float32
for i := range a {
sum += a[i] * b[i]
}
return sum
}

// EuclideanDistanceFMA falls back to scalar implementation on non-AMD64
func EuclideanDistanceFMA(a, b []float32) float32 {
if len(a) == 0 || len(a) != len(b) {
return 0
}
var sum float32
for i := range a {
diff := a[i] - b[i]
sum += diff * diff
}
return sum
}

// CosineDistanceFMA falls back to scalar implementation on non-AMD64
func CosineDistanceFMA(a, b []float32) float32 {
if len(a) == 0 || len(a) != len(b) {
return 1.0
}
var dot, normA, normB float64
for i := range a {
dot += float64(a[i]) * float64(b[i])
normA += float64(a[i]) * float64(a[i])
normB += float64(b[i]) * float64(b[i])
}
if normA == 0 || normB == 0 {
return 1.0
}
return float32(1.0 - dot/math.Sqrt(normA*normB))
}
