//go:build amd64

package simd

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

// The 8-way ILP AVX2 float64 kernels accumulate in float64 using a different
// summation order than the Go references and round to float32 exactly once.
// The tolerances below therefore only need to cover summation-order noise
// (~n * float64 eps) plus at most a few float32 ulp (1 ulp = 1.19e-7
// relative) introduced by that final rounding.
const (
	ilpParityRelTol = 1e-6
	ilpParityAbsTol = 1e-6
)

// ilpTestDims returns 1..300 plus the edge dimensions 0, 1, 3, 7, 8, 63, 64,
// 65, 127, 128 and 129 (deduplicated).
func ilpTestDims() []int {
	seen := make(map[int]bool, 340)
	dims := make([]int, 0, 340)
	add := func(d int) {
		if !seen[d] {
			seen[d] = true
			dims = append(dims, d)
		}
	}
	for _, d := range []int{0, 1, 3, 7, 8, 63, 64, 65, 127, 128, 129} {
		add(d)
	}
	for d := 1; d <= 300; d++ {
		add(d)
	}
	return dims
}

func ilpCheck(t *testing.T, name string, dims int, got, want float32) bool {
	t.Helper()
	diff := math.Abs(float64(got - want))
	tol := ilpParityAbsTol + ilpParityRelTol*math.Max(1, math.Abs(float64(want)))
	if diff > tol {
		t.Errorf("%s dims=%d: got=%v want=%v diff=%g tol=%g", name, dims, got, want, diff, tol)
		return false
	}
	return true
}

func TestEuclideanFloat64AVX2_ILPParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20240928))
	for _, d := range ilpTestDims() {
		va := make([]float64, d)
		vb := make([]float64, d)
		for i := range va {
			va[i] = rng.Float64() * 10
			vb[i] = rng.Float64() * 10
		}

		want, err := euclideanFloat64Unrolled4x(va, vb)
		if err != nil {
			t.Fatalf("dims=%d reference failed: %v", d, err)
		}

		got, err := euclideanFloat64AVX2(va, vb)
		if err != nil {
			t.Fatalf("dims=%d AVX2 kernel failed: %v", d, err)
		}
		if !ilpCheck(t, "euclideanFloat64AVX2", d, got, want) {
			continue
		}

		disp, err := EuclideanDistanceFloat64(va, vb)
		if err != nil {
			t.Fatalf("dims=%d dispatch failed: %v", d, err)
		}
		ilpCheck(t, "EuclideanDistanceFloat64", d, disp, want)
	}
}

func TestDotFloat64AVX2_ILPParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20240929))
	for _, d := range ilpTestDims() {
		va := make([]float64, d)
		vb := make([]float64, d)
		for i := range va {
			va[i] = rng.Float64() * 10
			vb[i] = rng.Float64() * 10
		}

		want, err := dotFloat64Unrolled4x(va, vb)
		if err != nil {
			t.Fatalf("dims=%d reference failed: %v", d, err)
		}

		got, err := dotFloat64AVX2(va, vb)
		if err != nil {
			t.Fatalf("dims=%d AVX2 kernel failed: %v", d, err)
		}
		ilpCheck(t, "dotFloat64AVX2", d, got, want)
	}
}

func TestEuclideanComplex128_ILPParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20240930))
	for _, d := range ilpTestDims() {
		va := make([]complex128, d)
		vb := make([]complex128, d)
		for i := range va {
			va[i] = complex(rng.Float64()*10, rng.Float64()*10)
			vb[i] = complex(rng.Float64()*10, rng.Float64()*10)
		}

		want, err := euclideanComplex128Unrolled(va, vb)
		if err != nil {
			t.Fatalf("dims=%d reference failed: %v", d, err)
		}

		got, err := EuclideanDistanceComplex128(va, vb)
		if err != nil {
			t.Fatalf("dims=%d dispatch failed: %v", d, err)
		}
		ilpCheck(t, "EuclideanDistanceComplex128", d, got, want)
	}
}

// TestAVX512Float64RuntimeGuard records which float64 path dispatch selected
// and proves that the AVX-512 wrappers never execute AVX-512 instructions when
// the CPU does not support them: on such hosts their results must be
// bit-identical to the AVX2 wrappers, because the guard routes them there.
func TestAVX512Float64RuntimeGuard(t *testing.T) {
	t.Logf("implementation=%q hasAVX512=%v hasAVX2=%v",
		implementation, features.HasAVX512, features.HasAVX2)

	selected := "unknown"
	switch reflect.ValueOf(euclideanDistanceFloat64Impl).Pointer() {
	case reflect.ValueOf(euclideanFloat64AVX2).Pointer():
		selected = "avx2"
	case reflect.ValueOf(euclideanFloat64AVX512).Pointer():
		selected = "avx512"
	}
	t.Logf("euclideanDistanceFloat64Impl selected path: %s", selected)

	if !features.HasAVX512 && implementation == "avx512" {
		t.Errorf("implementation %q selected without AVX-512 support", implementation)
	}
	if !features.HasAVX512 && selected != "avx2" {
		t.Errorf("float64 dispatch selected %q on a host without AVX-512", selected)
	}

	rng := rand.New(rand.NewSource(20240928))
	for _, d := range ilpTestDims() {
		va := make([]float64, d)
		vb := make([]float64, d)
		for i := range va {
			va[i] = rng.Float64() * 10
			vb[i] = rng.Float64() * 10
		}

		eucAVX2, err := euclideanFloat64AVX2(va, vb)
		if err != nil {
			t.Fatalf("dims=%d AVX2 failed: %v", d, err)
		}
		eucAVX512, err := euclideanFloat64AVX512(va, vb)
		if err != nil {
			t.Fatalf("dims=%d guarded AVX-512 failed: %v", d, err)
		}
		dotAVX2, err := dotFloat64AVX2(va, vb)
		if err != nil {
			t.Fatalf("dims=%d AVX2 dot failed: %v", d, err)
		}
		dotAVX512, err := dotFloat64AVX512(va, vb)
		if err != nil {
			t.Fatalf("dims=%d guarded AVX-512 dot failed: %v", d, err)
		}
		cosAVX2, err := cosineFloat64AVX2(va, vb)
		if err != nil {
			t.Fatalf("dims=%d AVX2 cosine failed: %v", d, err)
		}
		cosAVX512, err := cosineFloat64AVX512(va, vb)
		if err != nil {
			t.Fatalf("dims=%d guarded AVX-512 cosine failed: %v", d, err)
		}
		l2AVX2, err := l2SquaredFloat64AVX2(va, vb)
		if err != nil {
			t.Fatalf("dims=%d AVX2 l2sq failed: %v", d, err)
		}
		l2AVX512, err := l2SquaredFloat64AVX512(va, vb)
		if err != nil {
			t.Fatalf("dims=%d guarded AVX-512 l2sq failed: %v", d, err)
		}

		if features.HasAVX512 {
			if !ilpCheck(t, "euclideanFloat64AVX512", d, eucAVX512, eucAVX2) {
				continue
			}
			ilpCheck(t, "dotFloat64AVX512", d, dotAVX512, dotAVX2)
			ilpCheck(t, "cosineFloat64AVX512", d, cosAVX512, cosAVX2)
			ilpCheck(t, "l2SquaredFloat64AVX512", d, l2AVX512, l2AVX2)
			continue
		}

		if !float32BitsEqual(eucAVX512, eucAVX2) {
			t.Fatalf("dims=%d: guarded euclideanFloat64AVX512=%v != AVX2=%v without AVX-512 support",
				d, eucAVX512, eucAVX2)
		}
		if !float32BitsEqual(dotAVX512, dotAVX2) {
			t.Fatalf("dims=%d: guarded dotFloat64AVX512=%v != AVX2=%v without AVX-512 support",
				d, dotAVX512, dotAVX2)
		}
		if !float32BitsEqual(cosAVX512, cosAVX2) {
			t.Fatalf("dims=%d: guarded cosineFloat64AVX512=%v != AVX2=%v without AVX-512 support",
				d, cosAVX512, cosAVX2)
		}
		if !float32BitsEqual(l2AVX512, l2AVX2) {
			t.Fatalf("dims=%d: guarded l2SquaredFloat64AVX512=%v != AVX2=%v without AVX-512 support",
				d, l2AVX512, l2AVX2)
		}
	}
}

func float32BitsEqual(x, y float32) bool {
	return math.Float32bits(x) == math.Float32bits(y)
}

func TestILPParityToleranceDocumentation(t *testing.T) {
	t.Logf("float32 ulp relative=%g; test tolerances rel=%g abs=%g",
		float64(1.1920929e-7), ilpParityRelTol, ilpParityAbsTol)
	if ilpParityRelTol < 4*1.1920929e-7 {
		t.Errorf("relative tolerance %g is tighter than 4 float32 ulp", ilpParityRelTol)
	}
}
