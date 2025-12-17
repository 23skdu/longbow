package simd

import (
"math"

"github.com/klauspost/cpuid/v2"
)

// CPUFeatures contains detected CPU SIMD capabilities
type CPUFeatures struct {
Vendor    string
HasAVX2   bool
HasAVX512 bool
HasNEON   bool
}

// Function pointer types for dispatch
type (
distanceFunc      func(a, b []float32) float32
distanceBatchFunc func(query []float32, vectors [][]float32, results []float32)
)

var (
features       CPUFeatures
implementation string

// Function pointers initialized at startup - eliminates switch overhead in hot path
euclideanDistanceImpl      distanceFunc
cosineDistanceImpl         distanceFunc
dotProductImpl             distanceFunc
euclideanDistanceBatchImpl distanceBatchFunc
)

func init() {
detectCPU()
initializeDispatch()
}

func detectCPU() {
features = CPUFeatures{
Vendor:    cpuid.CPU.VendorString,
HasAVX2:   cpuid.CPU.Supports(cpuid.AVX2),
HasAVX512: cpuid.CPU.Supports(cpuid.AVX512F) && cpuid.CPU.Supports(cpuid.AVX512DQ),
HasNEON:   cpuid.CPU.Supports(cpuid.ASIMD), // ARM NEON
}

// Select best implementation
switch {
case features.HasAVX512:
implementation = "avx512"
case features.HasAVX2:
implementation = "avx2"
case features.HasNEON:
implementation = "neon"
default:
implementation = "generic"
}
}

// initializeDispatch sets function pointers based on detected CPU features.
// This is called once at startup, removing branch overhead from hot paths.
func initializeDispatch() {
switch implementation {
case "avx512":
euclideanDistanceImpl = euclideanAVX512
cosineDistanceImpl = cosineAVX512
dotProductImpl = dotAVX512
euclideanDistanceBatchImpl = euclideanBatchAVX512
case "avx2":
euclideanDistanceImpl = euclideanAVX2
cosineDistanceImpl = cosineAVX2
dotProductImpl = dotAVX2
euclideanDistanceBatchImpl = euclideanBatchAVX2
case "neon":
euclideanDistanceImpl = euclideanNEON
cosineDistanceImpl = cosineNEON
dotProductImpl = dotNEON
euclideanDistanceBatchImpl = euclideanBatchNEON
default:
euclideanDistanceImpl = euclideanGeneric
cosineDistanceImpl = cosineGeneric
dotProductImpl = dotGeneric
euclideanDistanceBatchImpl = euclideanBatchGeneric
}
}

// GetCPUFeatures returns detected CPU SIMD capabilities
func GetCPUFeatures() CPUFeatures {
return features
}

// GetImplementation returns the selected SIMD implementation name
func GetImplementation() string {
return implementation
}

// EuclideanDistance calculates the Euclidean distance between two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func EuclideanDistance(a, b []float32) float32 {
if len(a) != len(b) {
panic("simd: vector length mismatch")
}
if len(a) == 0 {
return 0
}
return euclideanDistanceImpl(a, b)
}

// CosineDistance calculates the cosine distance (1 - similarity) between two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func CosineDistance(a, b []float32) float32 {
if len(a) != len(b) {
panic("simd: vector length mismatch")
}
if len(a) == 0 {
return 1.0
}
return cosineDistanceImpl(a, b)
}

// DotProduct calculates the dot product of two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func DotProduct(a, b []float32) float32 {
if len(a) != len(b) {
panic("simd: vector length mismatch")
}
if len(a) == 0 {
return 0
}
return dotProductImpl(a, b)
}

// Generic implementations (fallback)

func euclideanGeneric(a, b []float32) float32 {
var sum float32
for i := 0; i < len(a); i++ {
d := a[i] - b[i]
sum += d * d
}
return float32(math.Sqrt(float64(sum)))
}

func cosineGeneric(a, b []float32) float32 {
var dot, normA, normB float32
for i := 0; i < len(a); i++ {
dot += a[i] * b[i]
normA += a[i] * a[i]
normB += b[i] * b[i]
}
if normA == 0 || normB == 0 {
return 1.0
}
return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

func dotGeneric(a, b []float32) float32 {
var sum float32
for i := 0; i < len(a); i++ {
sum += a[i] * b[i]
}
return sum
}

// EuclideanDistanceBatch calculates the Euclidean distance between a query vector and multiple candidate vectors.
// This batch version reduces function call overhead and allows for CPU-specific optimizations.
// Uses pre-selected implementation via function pointer (no switch overhead).
func EuclideanDistanceBatch(query []float32, vectors [][]float32, results []float32) {
if len(vectors) != len(results) {
panic("simd: vectors and results length mismatch")
}
if len(vectors) == 0 {
return
}
euclideanDistanceBatchImpl(query, vectors, results)
}

// euclideanBatchGeneric is the fallback implementation
func euclideanBatchGeneric(query []float32, vectors [][]float32, results []float32) {
for i, v := range vectors {
results[i] = euclideanGeneric(query, v)
}
}
