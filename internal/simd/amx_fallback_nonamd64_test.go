//go:build !amd64

package simd

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// TestAMXEntryPointsDelegateToNEON guards the non-x86 wiring for the AMX entry
// points. dispatch.go registers the emerald and granite tables on every
// architecture, so these symbols have to resolve everywhere even though
// detectCPU can only select them on x86. They must not be stubs that return
// zero: they have to agree with the kernels that are actually used.
func TestAMXEntryPointsDelegateToNEON(t *testing.T) {
	const dim = 64
	a := make([]float32, dim)
	b := make([]float32, dim)
	h := make([]float16.Num, dim)
	for i := range a {
		a[i] = float32(i) * 0.5
		b[i] = float32(dim-i) * 0.25
		h[i] = float16.New(a[i])
	}

	check := func(name string, got, want float32, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
		if math.Abs(float64(got-want)) > 1e-3 {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}

	got, err := euclideanAMX(a, b)
	want, _ := euclideanNEON(a, b)
	check("euclideanAMX", got, want, err)

	got, err = dotAMX(a, b)
	want, _ = dotNEON(a, b)
	check("dotAMX", got, want, err)

	got, err = l2SquaredAMX(a, b)
	want, _ = l2SquaredNEON(a, b)
	check("l2SquaredAMX", got, want, err)

	got, err = euclideanF16AMX(h, h)
	want, _ = euclideanF16NEON(h, h)
	check("euclideanF16AMX", got, want, err)

	got, err = dotF16AMX(h, h)
	want, _ = dotF16NEON(h, h)
	check("dotF16AMX", got, want, err)

	vectors := [][]float32{a, b, a}
	dist := make([]float32, len(vectors))
	if err := euclideanBatchAMX(a, vectors, dist); err != nil {
		t.Fatalf("euclideanBatchAMX: unexpected error: %v", err)
	}
	ref := make([]float32, len(vectors))
	if err := euclideanBatchNEON(a, vectors, ref); err != nil {
		t.Fatalf("euclideanBatchNEON: unexpected error: %v", err)
	}
	for i := range dist {
		if math.Abs(float64(dist[i]-ref[i])) > 1e-3 {
			t.Errorf("euclideanBatchAMX[%d]: got %v, want %v", i, dist[i], ref[i])
		}
	}

	dots := make([]float32, len(vectors))
	if err := dotBatchAMX(a, vectors, dots); err != nil {
		t.Fatalf("dotBatchAMX: unexpected error: %v", err)
	}
	refDots := make([]float32, len(vectors))
	if err := dotBatchNEON(a, vectors, refDots); err != nil {
		t.Fatalf("dotBatchNEON: unexpected error: %v", err)
	}
	for i := range dots {
		if math.Abs(float64(dots[i]-refDots[i])) > 1e-3 {
			t.Errorf("dotBatchAMX[%d]: got %v, want %v", i, dots[i], refDots[i])
		}
	}

	// matMulAMX must fill dst rather than leaving it untouched.
	m, n, k := 2, 3, 4
	lhs := make([]float32, m*k)
	rhs := make([]float32, k*n)
	for i := range lhs {
		lhs[i] = float32(i + 1)
	}
	for i := range rhs {
		rhs[i] = float32(2*i + 1)
	}
	dst := make([]float32, m*n)
	matMulAMX(lhs, rhs, m, n, k, dst)
	refMat := make([]float32, m*n)
	matMulNEON(lhs, rhs, m, n, k, refMat)
	for i := range dst {
		if math.Abs(float64(dst[i]-refMat[i])) > 1e-3 {
			t.Errorf("matMulAMX dst[%d]: got %v, want %v", i, dst[i], refMat[i])
		}
	}
}
