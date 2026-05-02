package simd

import (
	"testing"
)

func FuzzDistances(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{8, 7, 6, 5, 4, 3, 2, 1})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		if len(a) != len(b) || len(a) == 0 {
			return
		}
		
		// Test Float32 with varying dimensions including specialized ones
		dims := []int{len(a) / 4, 128, 384, 768, 1024, 1536, 3072}
		for _, d := range dims {
			if d <= 0 { continue }
			fa := make([]float32, d)
			fb := make([]float32, d)
			for i := 0; i < d; i++ {
				fa[i] = float32(i % 127) / 127.0
				fb[i] = float32((i + 1) % 127) / 127.0
			}
			
			_, err := EuclideanDistance(fa, fb)
			if err != nil && d == len(a)/4 { t.Errorf("EuclideanDistance failed: %v", err) }
			
			_, err = DotProduct(fa, fb)
			if err != nil && d == len(a)/4 { t.Errorf("DotProduct failed: %v", err) }
			
			_, err = CosineDistance(fa, fb)
			if err != nil && d == len(a)/4 { t.Errorf("CosineDistance failed: %v", err) }

			// Test Batch
			results := make([]float32, 2)
			vectors := [][]float32{fa, fb}
			_ = EuclideanDistanceBatch(fa, vectors, results)
			_ = DotProductBatch(fa, vectors, results)
			_ = CosineDistanceBatch(fa, vectors, results)
		}

		// Test Int8
		if len(a) > 0 {
			i8a := make([]int8, len(a))
			i8b := make([]int8, len(b))
			for i := range a {
				i8a[i] = int8(a[i])
				i8b[i] = int8(b[i])
			}
			_, _ = EuclideanDistanceInt8(i8a, i8b)
			_, _ = DotProductInt8(i8a, i8b)
		}
	})
}
