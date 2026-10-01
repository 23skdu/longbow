//go:build amd64

package tensor

import (
	"math"
	"math/rand"
	"testing"
)

func TestGemmCorrectness(t *testing.T) {
	sizes := []struct{ m, n, k int }{
		{4, 8, 1},
		{4, 8, 2},
		{4, 8, 7},
		{4, 8, 16},
		{8, 8, 8},
		{16, 16, 16},
		{32, 32, 32},
		{16, 32, 8},
		{7, 11, 5},
		{64, 64, 64},
		{100, 80, 60},
		{64, 64, 129},
		{128, 128, 256},
		{64, 64, 300},
	}
	rng := rand.New(rand.NewSource(42))
	for _, sz := range sizes {
		a := New(DtypeFloat32, Shape{sz.m, sz.k})
		b := New(DtypeFloat32, Shape{sz.k, sz.n})
		out := New(DtypeFloat32, Shape{sz.m, sz.n})
		for i := range a.Float32s() {
			a.Float32s()[i] = rng.Float32()
		}
		for i := range b.Float32s() {
			b.Float32s()[i] = rng.Float32()
		}
		// Compute via assembly-backed matMulTiledAMD64
		copy(out.Float32s(), make([]float32, out.NumElements()))
		ok := matMulTiledAMD64(a, b, out, sz.m, sz.n, sz.k)
		if !ok {
			t.Errorf("matMulTiledAMD64 returned false for %dx%dx%d", sz.m, sz.n, sz.k)
		}
		// Compute via generic matMulGeneric
		outRef := New(DtypeFloat32, Shape{sz.m, sz.n})
		ok = matMulGeneric(a, b, outRef, sz.m, sz.n, sz.k)
		if !ok {
			t.Errorf("matMulGeneric returned false for %dx%dx%d", sz.m, sz.n, sz.k)
		}
		// Compare with tolerance (different FMA accumulation orders)
		for i := range out.Float32s() {
			got := float64(out.Float32s()[i])
			want := float64(outRef.Float32s()[i])
			diff := got - want
			if diff < 0 {
				diff = -diff
			}
			rel := diff
			if want != 0 {
				rel = diff / math.Abs(want)
			}
			if rel > 1e-4 {
				t.Errorf("mismatch at [%d] for %dx%dx%d: got %f, want %f (diff %e, rel %e)",
					i, sz.m, sz.n, sz.k, got, want, diff, rel)
				break
			}
		}
	}
}
