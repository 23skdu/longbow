//go:build amd64 && !granite
// +build amd64,!granite

package simd

import "github.com/apache/arrow-go/v18/arrow/float16"

// Granite stubs for systems without the granite build tag.
// These fall back to generic implementations.

func euclideanF16AMX(a, b []float16.Num) (float32, error) {
	return euclideanF16Unrolled4x(a, b)
}

func dotF16AMX(a, b []float16.Num) (float32, error) {
	return dotF16Unrolled4x(a, b)
}

func matMulF16AMX(a, b []float16.Num, m, n, k int, dst []float16.Num) {
	fa := make([]float32, len(a))
	fb := make([]float32, len(b))
	fdst := make([]float32, len(dst))
	float16ToFloat32Generic(a, fa)
	float16ToFloat32Generic(b, fb)
	matMulGeneric(fa, fb, m, n, k, fdst)
	for i := range dst {
		dst[i] = float16.New(fdst[i])
	}
}

var _ = func() {
	if false {
		matMulF16AMX(nil, nil, 0, 0, 0, nil)
	}
}
