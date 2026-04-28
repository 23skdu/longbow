//go:build amd64
// +build amd64

package simd

import "unsafe"

// AVX2 optimized Euclidean distance
// Processes 8 float32s at a time (256-bit registers)
func euclideanAVX2(a, b []float32) (float32, error) {
	sum, err := l2SquaredAVX2(a, b)
	return float32(math.Sqrt(float64(sum))), err
}

// AVX2 optimized L2 Squared distance (no Sqrt)
func l2SquaredAVX2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return L2SquaredFloat32(a, b)
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

	return sum, nil
}

// dotAVX2 computes dot product using AVX2
func dotAVX2(a, b []float32) (float32, error) {
	// Implementation uses AVX2 256-bit operations
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return dotGeneric(a, b)
	}

	var sum float32
	n := len(a)
	i := 0

	// Process 8 elements at a time
	for ; i <= n-8; i += 8 {
		sum += dot8AVX2(
			unsafe.Pointer(&a[i]),
			unsafe.Pointer(&b[i]),
		)
	}

	// Handle remaining elements
	for ; i < n; i++ {
		sum += a[i] * b[i]
	}

	return sum, nil
}
