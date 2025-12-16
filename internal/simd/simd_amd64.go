//go:build amd64

package simd

import (
"math"
"unsafe"
)

// AVX2 optimized Euclidean distance
// Processes 8 float32s at a time (256-bit registers)
func euclideanAVX2(a, b []float32) float32 {
if !features.HasAVX2 {
return euclideanGeneric(a, b)
}

var sum float32
n := len(a)
i := 0

// Process 8 elements at a time (AVX2: 256-bit = 8 x float32)
for ; i <= n-8; i += 8 {
sum += euclidean8AVX2(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
}

// Handle remaining elements
for ; i < n; i++ {
d := a[i] - b[i]
sum += d * d
}

return float32(math.Sqrt(float64(sum)))
}

// AVX512 optimized Euclidean distance
// Processes 16 float32s at a time (512-bit registers)
func euclideanAVX512(a, b []float32) float32 {
if !features.HasAVX512 {
return euclideanAVX2(a, b)
}

var sum float32
n := len(a)
i := 0

// Process 16 elements at a time (AVX512: 512-bit = 16 x float32)
for ; i <= n-16; i += 16 {
sum += euclidean16AVX512(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
}

// Fall back to AVX2 for remaining 8+ elements
for ; i <= n-8; i += 8 {
sum += euclidean8AVX2(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
}

// Handle remaining elements
for ; i < n; i++ {
d := a[i] - b[i]
sum += d * d
}

return float32(math.Sqrt(float64(sum)))
}

// AVX2 optimized Cosine distance
func cosineAVX2(a, b []float32) float32 {
if !features.HasAVX2 {
return cosineGeneric(a, b)
}

var dot, normA, normB float32
n := len(a)
i := 0

// Process 8 elements at a time
for ; i <= n-8; i += 8 {
d, na, nb := cosine8AVX2(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
dot += d
normA += na
normB += nb
}

// Handle remaining elements
for ; i < n; i++ {
dot += a[i] * b[i]
normA += a[i] * a[i]
normB += b[i] * b[i]
}

if normA == 0 || normB == 0 {
return 1.0
}
return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

// AVX512 optimized Cosine distance
func cosineAVX512(a, b []float32) float32 {
if !features.HasAVX512 {
return cosineAVX2(a, b)
}

var dot, normA, normB float32
n := len(a)
i := 0

// Process 16 elements at a time
for ; i <= n-16; i += 16 {
d, na, nb := cosine16AVX512(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
dot += d
normA += na
normB += nb
}

// Fall back to AVX2
for ; i <= n-8; i += 8 {
d, na, nb := cosine8AVX2(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
dot += d
normA += na
normB += nb
}

// Handle remaining
for ; i < n; i++ {
dot += a[i] * b[i]
normA += a[i] * a[i]
normB += b[i] * b[i]
}

if normA == 0 || normB == 0 {
return 1.0
}
return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

// AVX2 optimized dot product
func dotAVX2(a, b []float32) float32 {
if !features.HasAVX2 {
return dotGeneric(a, b)
}

var sum float32
n := len(a)
i := 0

for ; i <= n-8; i += 8 {
sum += dot8AVX2(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
}

for ; i < n; i++ {
sum += a[i] * b[i]
}

return sum
}

// AVX512 optimized dot product
func dotAVX512(a, b []float32) float32 {
if !features.HasAVX512 {
return dotAVX2(a, b)
}

var sum float32
n := len(a)
i := 0

for ; i <= n-16; i += 16 {
sum += dot16AVX512(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
}

for ; i <= n-8; i += 8 {
sum += dot8AVX2(
unsafe.Pointer(&a[i]),
unsafe.Pointer(&b[i]),
)
}

for ; i < n; i++ {
sum += a[i] * b[i]
}

return sum
}

// NEON stubs for AMD64 (not available)
func euclideanNEON(a, b []float32) float32 {
return euclideanGeneric(a, b)
}

func cosineNEON(a, b []float32) float32 {
return cosineGeneric(a, b)
}

func dotNEON(a, b []float32) float32 {
return dotGeneric(a, b)
}

// Assembly function declarations (implemented in simd_amd64.s)
// These compute partial sums for 8/16 element chunks

//go:noescape
func euclidean8AVX2(a, b unsafe.Pointer) float32

//go:noescape
func euclidean16AVX512(a, b unsafe.Pointer) float32

//go:noescape
func cosine8AVX2(a, b unsafe.Pointer) (dot, normA, normB float32)

//go:noescape
func cosine16AVX512(a, b unsafe.Pointer) (dot, normA, normB float32)

//go:noescape
func dot8AVX2(a, b unsafe.Pointer) float32

//go:noescape
func dot16AVX512(a, b unsafe.Pointer) float32
