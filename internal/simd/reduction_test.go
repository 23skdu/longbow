package simd

import (
	"math"
	"math/rand"
	"testing"
)

func TestReductions(t *testing.T) {
	sizes := []int{1, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 100, 1024}
	for _, n := range sizes {
		t.Run(string(rune(n)), func(t *testing.T) {
			src := make([]float32, n)
			var wantSum float32
			wantMax := float32(-math.MaxFloat32)
			wantMin := float32(math.MaxFloat32)

			for i := range src {
				src[i] = rand.Float32()*200 - 100
				wantSum += src[i]
				if src[i] > wantMax {
					wantMax = src[i]
				}
				if src[i] < wantMin {
					wantMin = src[i]
				}
			}

			gotSum := Sum(src)
			gotMax := Max(src)
			gotMin := Min(src)

			if math.Abs(float64(gotSum-wantSum)) > 1e-2 || gotMax != wantMax || gotMin != wantMin {
				t.Errorf("n=%d: Sum(got=%f, want=%f), Max(got=%f, want=%f), Min(got=%f, want=%f)",
					n, gotSum, wantSum, gotMax, wantMax, gotMin, wantMin)
			}
		})
	}
}
