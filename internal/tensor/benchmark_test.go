package tensor

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkMatMul(b *testing.B) {
	sizes := []struct{ m, n, k int }{
		{16, 16, 16},
		{32, 32, 32},
		{64, 64, 64},
		{128, 128, 128},
		{256, 256, 256},
		{512, 512, 512},
		{1024, 1024, 1024},
		{64, 128, 64},
		{128, 256, 128},
	}

	for _, sz := range sizes {
		name := fmt.Sprintf("%dx%dx%d", sz.m, sz.n, sz.k)
		b.Run(name, func(b *testing.B) {
			a := New(DtypeFloat32, Shape{sz.m, sz.k})
			bt := New(DtypeFloat32, Shape{sz.k, sz.n})
			out := New(DtypeFloat32, Shape{sz.m, sz.n})
			rng := rand.New(rand.NewSource(42))
			for i := range a.Float32s() {
				a.Float32s()[i] = rng.Float32()
			}
			for i := range bt.Float32s() {
				bt.Float32s()[i] = rng.Float32()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				contractSIMDMatMul(a, bt, out, sz.m, sz.n, sz.k)
			}
		})
	}
}

func BenchmarkMatMulGeneric(b *testing.B) {
	sizes := []struct{ m, n, k int }{
		{64, 64, 64},
		{128, 128, 128},
		{256, 256, 256},
	}

	for _, sz := range sizes {
		name := fmt.Sprintf("%dx%dx%d", sz.m, sz.n, sz.k)
		b.Run(name, func(b *testing.B) {
			a := New(DtypeFloat32, Shape{sz.m, sz.k})
			bt := New(DtypeFloat32, Shape{sz.k, sz.n})
			out := New(DtypeFloat32, Shape{sz.m, sz.n})
			rng := rand.New(rand.NewSource(42))
			for i := range a.Float32s() {
				a.Float32s()[i] = rng.Float32()
			}
			for i := range bt.Float32s() {
				bt.Float32s()[i] = rng.Float32()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				matMulGeneric(a, bt, out, sz.m, sz.n, sz.k)
			}
		})
	}
}
