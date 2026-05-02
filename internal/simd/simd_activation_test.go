package simd

import (
	"fmt"
	"math"
	"runtime"
	"testing"
)

func expSIMD(src, dst []float32) {
	Exp(src, dst)
}

func softmaxSIMD(src, dst []float32) {
	Softmax(src, dst)
}

func TestExpSIMD(t *testing.T) {
	testSizes := []int{1, 4, 15, 16, 17, 31, 32, 33, 100, 128}
	for _, n := range testSizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			src := make([]float32, n)
			for i := range src {
				src[i] = float32(i) / 10.0
			}
			dstSIMD := make([]float32, n)
			dstGeneric := make([]float32, n)

			expSIMD(src, dstSIMD)
			expGeneric(src, dstGeneric)

			for i := range src {
				diff := math.Abs(float64(dstSIMD[i] - dstGeneric[i]))
				relDiff := diff / math.Max(1.0, float64(dstGeneric[i]))
				if relDiff > 0.02 { // 2% tolerance for polynomial approximation
					t.Errorf("index %d: src=%f, SIMD=%f, Generic=%f, relDiff=%f",
						i, src[i], dstSIMD[i], dstGeneric[i], relDiff)
				}
			}
		})
	}
}

func TestSoftmaxSIMD(t *testing.T) {
	testSizes := []int{1, 4, 15, 16, 17, 31, 32, 33, 100, 128}
	for _, n := range testSizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			src := make([]float32, n)
			for i := range src {
				src[i] = float32(i)
			}
			dstSIMD := make([]float32, n)
			dstGeneric := make([]float32, n)

			softmaxSIMD(src, dstSIMD)
			softmaxGeneric(src, dstGeneric)

			var sumSIMD float32
			for i := range src {
				sumSIMD += dstSIMD[i]
				diff := math.Abs(float64(dstSIMD[i] - dstGeneric[i]))
				if diff > 0.01 {
					t.Errorf("index %d: src=%f, SIMD=%f, Generic=%f",
						i, src[i], dstSIMD[i], dstGeneric[i])
				}
			}
			if math.Abs(float64(sumSIMD-1.0)) > 0.001 {
				t.Errorf("SIMD softmax sum should be ~1.0, got %f", sumSIMD)
			}
		})
	}
}

func TestExpSIMDNegative(t *testing.T) {
	// Ensure negative inputs and large inputs don't produce NaN/Inf
	src := []float32{-10, -1, 0, 1, 5, 10}
	dst := make([]float32, len(src))
	dstRef := make([]float32, len(src))
	expSIMD(src, dst)
	expGeneric(src, dstRef)
	for i, v := range dst {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			t.Errorf("index %d: SIMD exp(%f) = %f", i, src[i], v)
		}
		relDiff := math.Abs(float64(v-dstRef[i])) / math.Max(1.0, float64(dstRef[i]))
		if relDiff > 0.05 {
			t.Errorf("index %d: SIMD=%f Generic=%f relDiff=%f", i, v, dstRef[i], relDiff)
		}
	}
}

func BenchmarkExpSIMD(b *testing.B) {
	const n = 1024
	src := make([]float32, n)
	for i := range src {
		src[i] = float32(i) / 100.0
	}
	dst := make([]float32, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expSIMD(src, dst)
	}
	b.SetBytes(int64(n * 4))
}

func BenchmarkExpGeneric(b *testing.B) {
	const n = 1024
	src := make([]float32, n)
	for i := range src {
		src[i] = float32(i) / 100.0
	}
	dst := make([]float32, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expGeneric(src, dst)
	}
	b.SetBytes(int64(n * 4))
}

func BenchmarkSoftmaxSIMD(b *testing.B) {
	const n = 1024
	src := make([]float32, n)
	for i := range src {
		src[i] = float32(i) / 100.0
	}
	dst := make([]float32, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		softmaxSIMD(src, dst)
	}
	b.SetBytes(int64(n * 4))
}

func BenchmarkSoftmaxGeneric(b *testing.B) {
	const n = 1024
	src := make([]float32, n)
	for i := range src {
		src[i] = float32(i) / 100.0
	}
	dst := make([]float32, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		softmaxGeneric(src, dst)
	}
	b.SetBytes(int64(n * 4))
}

func TestSigmoidSIMD(t *testing.T) {
	testSizes := []int{1, 4, 15, 16, 17, 31, 32, 33, 100, 128}
	for _, n := range testSizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			src := make([]float32, n)
			for i := range src {
				src[i] = float32(i)/10.0 - 5.0 // Range [-5, 7.8]
			}
			dstSIMD := make([]float32, n)
			dstGeneric := make([]float32, n)

			sigmoidSIMD(src, dstSIMD)
			sigmoidGeneric(src, dstGeneric)

			for i := range src {
				diff := math.Abs(float64(dstSIMD[i] - dstGeneric[i]))
				if diff > 0.05 {
					t.Errorf("index %d: src=%f, SIMD=%f, Generic=%f",
						i, src[i], dstSIMD[i], dstGeneric[i])
				}
			}
		})
	}
}

func TestLogSIMD(t *testing.T) {
	testSizes := []int{1, 4, 15, 16, 17, 31, 32, 33, 100, 128}
	for _, n := range testSizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			src := make([]float32, n)
			for i := range src {
				src[i] = float32(i)/100.0 + 0.5 // Range [0.5, 1.78]
			}
			dstSIMD := make([]float32, n)
			dstGeneric := make([]float32, n)

			logSIMD(src, dstSIMD)
			logGeneric(src, dstGeneric)

			for i := range src {
				diff := math.Abs(float64(dstSIMD[i] - dstGeneric[i]))
				relDiff := diff / math.Max(1.0, float64(dstGeneric[i]))
				if relDiff > 0.25 {
					t.Errorf("index %d: src=%f, SIMD=%f, Generic=%f",
						i, src[i], dstSIMD[i], dstGeneric[i])
				}
			}
		})
	}
}

func sigmoidSIMD(src, dst []float32) {
	Sigmoid(src, dst)
}

func logSIMD(src, dst []float32) {
	Log(src, dst)
}
