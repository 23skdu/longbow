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

var (
features     CPUFeatures
implementation string
)

func init() {
detectCPU()
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

// GetCPUFeatures returns detected CPU SIMD capabilities
func GetCPUFeatures() CPUFeatures {
return features
}

// GetImplementation returns the selected SIMD implementation name
func GetImplementation() string {
return implementation
}

// EuclideanDistance calculates the Euclidean distance between two vectors
func EuclideanDistance(a, b []float32) float32 {
if len(a) != len(b) {
panic("simd: vector length mismatch")
}
if len(a) == 0 {
return 0
}

switch implementation {
case "avx512":
return euclideanAVX512(a, b)
case "avx2":
return euclideanAVX2(a, b)
case "neon":
return euclideanNEON(a, b)
default:
return euclideanGeneric(a, b)
}
}

// CosineDistance calculates the cosine distance (1 - similarity) between two vectors
func CosineDistance(a, b []float32) float32 {
if len(a) != len(b) {
panic("simd: vector length mismatch")
}
if len(a) == 0 {
return 1.0
}

switch implementation {
case "avx512":
return cosineAVX512(a, b)
case "avx2":
return cosineAVX2(a, b)
case "neon":
return cosineNEON(a, b)
default:
return cosineGeneric(a, b)
}
}

// DotProduct calculates the dot product of two vectors
func DotProduct(a, b []float32) float32 {
if len(a) != len(b) {
panic("simd: vector length mismatch")
}
if len(a) == 0 {
return 0
}

switch implementation {
case "avx512":
return dotAVX512(a, b)
case "avx2":
return dotAVX2(a, b)
case "neon":
return dotNEON(a, b)
default:
return dotGeneric(a, b)
}
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
