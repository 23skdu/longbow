package simd

import (
	"math"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// Activation Functions

// Sigmoid applies the sigmoid activation function element-wise.
func Sigmoid(src, dst []float32) {
	start := time.Now()
	if sigmoidFloat32Impl != nil {
		sigmoidFloat32Impl(src, dst)
	} else {
		sigmoidGeneric(src, dst)
	}
	metrics.SIMDActivationDuration.WithLabelValues("sigmoid", implementation).Observe(time.Since(start).Seconds())
}

// Softmax applies the softmax activation function to the input slice.
func Softmax(src, dst []float32) {
	start := time.Now()
	if softmaxFloat32Impl != nil {
		softmaxFloat32Impl(src, dst)
	} else {
		softmaxGeneric(src, dst)
	}
	metrics.SIMDActivationDuration.WithLabelValues("softmax", implementation).Observe(time.Since(start).Seconds())
}

// Exp applies the exponential function element-wise.
func Exp(src, dst []float32) {
	start := time.Now()
	if expFloat32Impl != nil {
		expFloat32Impl(src, dst)
	} else {
		expGeneric(src, dst)
	}
	metrics.SIMDActivationDuration.WithLabelValues("exp", implementation).Observe(time.Since(start).Seconds())
}

// Log applies the natural logarithm function element-wise.
func Log(src, dst []float32) {
	start := time.Now()
	if logFloat32Impl != nil {
		logFloat32Impl(src, dst)
	} else {
		logGeneric(src, dst)
	}
	metrics.SIMDActivationDuration.WithLabelValues("log", implementation).Observe(time.Since(start).Seconds())
}

// AccumulateWeightedScatter adds weighted values to a destination slice using scatter indices.
func AccumulateWeightedScatter(dst []float32, targets []uint32, weights []float32, factor float32) {
	accumulateWeightedScatterFloat32Impl(dst, targets, weights, factor)
}

func sigmoidGeneric(src, dst []float32) {
	for i, x := range src {
		dst[i] = 1.0 / (1.0 + float32(math.Exp(float64(-x))))
	}
}

func expGeneric(src, dst []float32) {
	for i, x := range src {
		dst[i] = float32(math.Exp(float64(x)))
	}
}

func logGeneric(src, dst []float32) {
	for i, x := range src {
		dst[i] = float32(math.Log(float64(x)))
	}
}

func softmaxGeneric(src, dst []float32) {
	var max float32 = -math.MaxFloat32
	for _, x := range src {
		if x > max {
			max = x
		}
	}
	var sum float32
	for i, x := range src {
		dst[i] = float32(math.Exp(float64(x - max)))
		sum += dst[i]
	}
	for i := range dst {
		dst[i] /= sum
	}
}

func sumGeneric(src []float32) float32 {
	var sum float32
	for _, x := range src {
		sum += x
	}
	return sum
}

func maxGeneric(src []float32) float32 {
	if len(src) == 0 {
		return -math.MaxFloat32
	}
	max := src[0]
	for _, x := range src[1:] {
		if x > max {
			max = x
		}
	}
	return max
}

func minGeneric(src []float32) float32 {
	if len(src) == 0 {
		return math.MaxFloat32
	}
	min := src[0]
	for _, x := range src[1:] {
		if x < min {
			min = x
		}
	}
	return min
}

// Sum calculates the sum of all elements in a float32 slice.
func Sum(src []float32) float32 {
	return sumFloat32Impl(src)
}

// Max finds the maximum value in a float32 slice.
func Max(src []float32) float32 {
	return maxFloat32Impl(src)
}

// Min finds the minimum value in a float32 slice.
func Min(src []float32) float32 {
	return minFloat32Impl(src)
}

// ArgMax returns the index of the maximum value in a float32 slice.
func ArgMax(src []float32) int {
	return argMaxFloat32Impl(src)
}

// ArgMin returns the index of the minimum value in a float32 slice.
func ArgMin(src []float32) int {
	return argMinFloat32Impl(src)
}

func argMaxGeneric(src []float32) int {
	if len(src) == 0 {
		return -1
	}
	maxIdx := 0
	maxVal := src[0]
	for i, x := range src[1:] {
		if x > maxVal {
			maxVal = x
			maxIdx = i + 1
		}
	}
	return maxIdx
}

func argMinGeneric(src []float32) int {
	if len(src) == 0 {
		return -1
	}
	minIdx := 0
	minVal := src[0]
	for i, x := range src[1:] {
		if x < minVal {
			minVal = x
			minIdx = i + 1
		}
	}
	return minIdx
}

func matMulGeneric(a, b []float32, m, n, k int, dst []float32) {
	// a: m x k, b: k x n, dst: m x n
	for i := 0; i < m; i++ {
		for j := 0; j < n; j++ {
			var sum float32
			for l := 0; l < k; l++ {
				sum += a[i*k+l] * b[l*n+j]
			}
			dst[i*n+j] = sum
		}
	}
}
func memcpyGeneric(dst, src unsafe.Pointer, n int) {
	d := unsafe.Slice((*byte)(dst), n) // #nosec G103
	s := unsafe.Slice((*byte)(src), n) // #nosec G103
	copy(d, s)
}

// MemcpyNTA performs a memory copy using non-temporal hints to avoid cache pollution.
func MemcpyNTA(dst, src unsafe.Pointer, n int) {
	memcpyNTAImpl(dst, src, n)
}
