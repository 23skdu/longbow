package simd

import (
	"github.com/apache/arrow-go/v18/arrow/float16"
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
			if d <= 0 {
				continue
			}
			fa := make([]float32, d)
			fb := make([]float32, d)
			for i := 0; i < d; i++ {
				fa[i] = float32(i%127) / 127.0
				fb[i] = float32((i+1)%127) / 127.0
			}

			_, err := EuclideanDistance(fa, fb)
			if err != nil && d == len(a)/4 {
				t.Errorf("EuclideanDistance failed: %v", err)
			}

			_, err = DotProduct(fa, fb)
			if err != nil && d == len(a)/4 {
				t.Errorf("DotProduct failed: %v", err)
			}

			_, err = CosineDistance(fa, fb)
			if err != nil && d == len(a)/4 {
				t.Errorf("CosineDistance failed: %v", err)
			}

			// Test Batch
			results := make([]float32, 2)
			vectors := [][]float32{fa, fb}
			_ = EuclideanDistanceBatch(fa, vectors, results)
			_ = DotProductBatch(fa, vectors, results)
			_ = CosineDistanceBatch(fa, vectors, results)
		}

		// Test Float64
		if len(a) >= 8 {
			d := len(a) / 8
			f64a := make([]float64, d)
			f64b := make([]float64, d)
			for i := 0; i < d; i++ {
				f64a[i] = float64(a[i])
				f64b[i] = float64(b[i])
			}
			_, _ = EuclideanDistanceFloat64(f64a, f64b)
			_, _ = DotProductF64(f64a, f64b)
			_, _ = CosineDistanceFloat64(f64a, f64b)
		}

		// Test Float16
		if len(a) >= 2 {
			d := len(a) / 2
			f16a := make([]float16.Num, d)
			f16b := make([]float16.Num, d)
			for i := 0; i < d; i++ {
				f16a[i] = float16.New(float32(a[i]))
				f16b[i] = float16.New(float32(b[i]))
			}
			_, _ = EuclideanDistanceF16(f16a, f16b)
			_, _ = DotProductF16(f16a, f16b)
			_, _ = CosineDistanceF16(f16a, f16b)
		}

		// Test Complex64
		if len(a) >= 8 {
			d := len(a) / 8
			c64a := make([]complex64, d)
			c64b := make([]complex64, d)
			for i := 0; i < d; i++ {
				c64a[i] = complex(float32(a[2*i]), float32(a[2*i+1]))
				c64b[i] = complex(float32(b[2*i]), float32(b[2*i+1]))
			}
			_, _ = EuclideanDistanceComplex64(c64a, c64b)
			_, _ = DotProductComplex64(c64a, c64b)
		}

		// Test Complex128
		if len(a) >= 16 {
			d := len(a) / 16
			c128a := make([]complex128, d)
			c128b := make([]complex128, d)
			for i := 0; i < d; i++ {
				c128a[i] = complex(float64(a[2*i]), float64(a[2*i+1]))
				c128b[i] = complex(float64(b[2*i]), float64(b[2*i+1]))
			}
			_, _ = EuclideanDistanceComplex128(c128a, c128b)
			_, _ = DotProductComplex128(c128a, c128b)
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

		// Test Int16
		if len(a) >= 2 {
			d := len(a) / 2
			i16a := make([]int16, d)
			i16b := make([]int16, d)
			for i := 0; i < d; i++ {
				i16a[i] = int16(a[2*i]) | (int16(a[2*i+1]) << 8)
				i16b[i] = int16(b[2*i]) | (int16(b[2*i+1]) << 8)
			}
			_, _ = EuclideanDistanceInt16(i16a, i16b)
			_, _ = DotProductInt16(i16a, i16b)
		}

		// Test Uint16
		if len(a) >= 2 {
			d := len(a) / 2
			u16a := make([]uint16, d)
			u16b := make([]uint16, d)
			for i := 0; i < d; i++ {
				u16a[i] = uint16(a[2*i]) | (uint16(a[2*i+1]) << 8)
				u16b[i] = uint16(b[2*i]) | (uint16(b[2*i+1]) << 8)
			}
			_, _ = EuclideanDistanceUint16(u16a, u16b)
			_, _ = DotProductUint16(u16a, u16b)
		}

		// Test Uint8
		if len(a) > 0 {
			u8a := make([]uint8, len(a))
			u8b := make([]uint8, len(b))
			for i := range a {
				u8a[i] = uint8(a[i])
				u8b[i] = uint8(b[i])
			}
			_, _ = EuclideanDistanceUint8(u8a, u8b)
			_, _ = DotProductUint8(u8a, u8b)
		}
	})
}
