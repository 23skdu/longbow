package simd

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestArgMaxCorrectness(t *testing.T) {
	for _, n := range []int{1, 7, 8, 15, 16, 31, 32, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			src := make([]float32, n)
			for i := range src {
				src[i] = rand.Float32()
			}
			
			// Set a known max
			expectedIdx := rand.Intn(n)
			src[expectedIdx] = 100.0
			
			actualIdx := ArgMax(src)
			if actualIdx != expectedIdx {
				t.Errorf("expected %d, got %d", expectedIdx, actualIdx)
			}
		})
	}
}

func TestArgMinCorrectness(t *testing.T) {
	for _, n := range []int{1, 7, 8, 15, 16, 31, 32, 100, 1000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			src := make([]float32, n)
			for i := range src {
				src[i] = rand.Float32() + 10.0
			}
			
			expectedIdx := rand.Intn(n)
			src[expectedIdx] = -1.0
			
			actualIdx := ArgMin(src)
			if actualIdx != expectedIdx {
				t.Errorf("expected %d, got %d", expectedIdx, actualIdx)
			}
		})
	}
}

func TestMatMulCorrectness(t *testing.T) {
	m, n, k := 4, 8, 4 // n must be multiple of 8 for our AVX2 kernel
	a := make([]float32, m*k)
	b := make([]float32, k*n)
	dstActual := make([]float32, m*n)
	dstExpected := make([]float32, m*n)
	
	for i := range a { a[i] = rand.Float32() }
	for i := range b { b[i] = rand.Float32() }
	
	MatMul(a, b, m, n, k, dstActual)
	matMulGeneric(a, b, m, n, k, dstExpected)
	
	for i := range dstActual {
		if !almostEqualReduction(dstActual[i], dstExpected[i]) {
			t.Errorf("at index %d: expected %f, got %f", i, dstExpected[i], dstActual[i])
		}
	}
}

func almostEqualReduction(a, b float32) bool {
	diff := a - b
	if diff < 0 { diff = -diff }
	return diff < 1e-5
}
