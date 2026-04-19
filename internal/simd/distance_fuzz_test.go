package simd

import (
	"testing"
	"unsafe"
)

func FuzzDistances(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4}, []byte{4, 3, 2, 1})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		if len(a) != len(b) || len(a) == 0 {
			return
		}
		
		// Test Float32
		if len(a) % 4 == 0 {
			count := len(a) / 4
			fa := make([]float32, count)
			fb := make([]float32, count)
			for i := 0; i < count; i++ {
				// Avoid NaN/Inf for stability in basic fuzzing
				fa[i] = float32(a[i*4]) / 255.0
				fb[i] = float32(b[i*4]) / 255.0
			}
			_, _ = EuclideanDistance(fa, fb)
			_, _ = CosineDistance(fa, fb)
			_, _ = DotProduct(fa, fb)
		}

		// Test Int8
		i8a := *(*[]int8)(unsafe.Pointer(&a)) // #nosec G103
		i8b := *(*[]int8)(unsafe.Pointer(&b)) // #nosec G103
		_, _ = EuclideanDistanceInt8(i8a, i8b)
		_, _ = CosineDistanceInt8(i8a, i8b)
		_, _ = DotProductInt8(i8a, i8b)
	})
}
