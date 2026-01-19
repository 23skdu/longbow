package simd

import (
	"math/rand"
	"testing"
	"time"
)

// BenchmarkHighDimTypes runs comparative benchmarks for different data types
// at "OpenAI scale" dimensions (1536, 3072) to measure effectiveness of SIMD.

const (
	dim1536 = 1536 // OpenAI text-embedding-3-small
	dim3072 = 3072 // OpenAI text-embedding-3-large
)

func BenchmarkHighDim_Float64_1536(b *testing.B) {
	benchmarkFloat64(b, dim1536)
}

func BenchmarkHighDim_Float64_3072(b *testing.B) {
	benchmarkFloat64(b, dim3072)
}

func BenchmarkHighDim_Int8_1536(b *testing.B) {
	benchmarkInt8(b, dim1536)
}

func BenchmarkHighDim_Int8_3072(b *testing.B) {
	benchmarkInt8(b, dim3072)
}

func BenchmarkHighDim_Int16_1536(b *testing.B) {
	benchmarkInt16(b, dim1536)
}

func BenchmarkHighDim_Int16_3072(b *testing.B) {
	benchmarkInt16(b, dim3072)
}

func BenchmarkHighDim_Complex128_1536(b *testing.B) {
	benchmarkComplex128(b, dim1536)
}

func BenchmarkHighDim_Complex128_3072(b *testing.B) {
	benchmarkComplex128(b, dim3072)
}

// Helpers

func benchmarkFloat64(b *testing.B, dim int) {
	v1 := make([]float64, dim)
	v2 := make([]float64, dim)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < dim; i++ {
		v1[i] = rng.Float64()
		v2[i] = rng.Float64()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistanceFloat64(v1, v2)
	}
}

func benchmarkInt8(b *testing.B, dim int) {
	v1 := make([]int8, dim)
	v2 := make([]int8, dim)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < dim; i++ {
		v1[i] = int8(rng.Intn(256) - 128)
		v2[i] = int8(rng.Intn(256) - 128)
	}

	// Registry lookup for generic int8 distance, or we need to expose specialized func?
	// simd.go doesn't expose EuclideanDistanceInt8 directly, it uses Registry.
	// But we are inside `simd` package so we can test implementations directly if generic?
	// The registry returns a generic interface function.

	// However, we want to test the *optimized* path once implemented.
	// For now, let's look up from registry to simulate real usage.

	// We need to pass pointers to generic function, which wants unsafe.Pointer or interface?
	// Registry returns func(unsafe.Pointer, unsafe.Pointer) float32 ? NO.
	// Registry returns `interface{}` currently or `unsafe.Pointer`?
	// Let's check `Registry.LookUp` signature from `registry.go`?
	// Wait, simd.go Registry.Register passes `euclideanInt8Unrolled4x` which is `func([]int8, []int8) float32`.
	// The Registry likely stores `interface{}`.

	// Reflection overhead is bad for microbenchmark.
	// We should probably rely on `euclideanInt8Unrolled4x` being available since we are in `package simd`.
	// But later we will add `euclideanInt8AVX2` and want to test THAT if enabled.
	// So we should use a dispatcher for Int8 if we add one, OR just call the function pointer that we will add.

	// FOR NOW: Let's assume we will add `EuclideanDistanceInt8` to `simd.go` like we have `EuclideanDistanceFloat64`.
	// If it doesn't exist, we will fail compilation.
	// Let's CHECK `simd.go` again... it does NOT have `EuclideanDistanceInt8` exported function.
	// It only has `EuclideanDistanceFloat64`.

	// So for Int8, we must access the internal `euclideanInt8Unrolled4x` or whatever dispatch variable we create.
	// Implementation plan said: "Add euclideanInt8AVX2" to simd_amd64.go.
	// And "Update initializeDispatch to wire up".

	// So we should likely add `var euclideanDistanceInt8Impl func([]int8, []int8) float32` to `simd.go`
	// and use that in benchmarks.

	// Fallback if Get not found
	distFunc := Registry.Get(MetricEuclidean, DataTypeInt8, 0)
	var f func([]int8, []int8) float32
	if distFunc == nil {
		f = euclideanInt8Unrolled4x
	} else {
		f = distFunc.(func([]int8, []int8) float32)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(v1, v2)
	}
}

func benchmarkInt16(b *testing.B, dim int) {
	v1 := make([]int16, dim)
	v2 := make([]int16, dim)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < dim; i++ {
		v1[i] = int16(rng.Intn(65536) - 32768)
		v2[i] = int16(rng.Intn(65536) - 32768)
	}

	// Use generic baseline for now since Registry might not expose what we need easily or lookup is complex.
	// Actually Registry.Get is likely what we want.
	// Let's check registry.go content.
	// Assuming Registry.Get(MetricEuclidean, DataTypeInt8, 0)

	// Fallback if Get not found
	distFunc := Registry.Get(MetricEuclidean, DataTypeInt16, 0)
	var f func([]int16, []int16) float32
	if distFunc == nil {
		// Fallback to internal if registry miss (shouldn't happen if initialized)
		f = euclideanInt16Unrolled4x
	} else {
		f = distFunc.(func([]int16, []int16) float32)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(v1, v2)
	}
}

func benchmarkComplex128(b *testing.B, dim int) {
	v1 := make([]complex128, dim)
	v2 := make([]complex128, dim)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < dim; i++ {
		v1[i] = complex(rng.Float64(), rng.Float64())
		v2[i] = complex(rng.Float64(), rng.Float64())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistanceComplex128(v1, v2)
	}
}
