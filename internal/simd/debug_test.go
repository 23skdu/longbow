//go:build amd64

package simd

import (
	"fmt"
	"testing"
	"math/rand"
	"unsafe"
)

func TestDebugInt32(t *testing.T) {
	rand.Seed(42)
	for _, d := range []int{4, 8, 16} {
		a := make([]int32, d)
		b := make([]int32, d)
		for i := 0; i < d; i++ {
			a[i] = rand.Int31() / 1000
			b[i] = rand.Int31() / 1000
		}
		
		// Compute reference with int64
		var refI64 int64
		for i := 0; i < d; i++ {
			refI64 += int64(a[i]) * int64(b[i])
		}
		refF32 := float32(refI64)
		
		// Call kernel directly
		got := dotInt32AVX2Kernel(
			uintptr(unsafe.Pointer(&a[0])),
			uintptr(unsafe.Pointer(&b[0])),
			d,
		)
		
		// Also test through DispatchDistance
		gotDD, _ := DispatchDistance(MetricDotProduct, a, b)
		
		fmt.Printf("dim=%d:\n", d)
		fmt.Printf("  a=%v\n", a)
		fmt.Printf("  b=%v\n", b)
		// Manually compute partial sums
		var s0, s1, s2, s3 int64
		for i := 0; i < d; i++ {
			prod := int64(a[i]) * int64(b[i])
			switch i % 4 {
			case 0: s0 += prod
			case 1: s0 += prod
			case 2: s1 += prod
			case 3: s1 += prod
			}
			_ = s2; _ = s3
		}
		fmt.Printf("  expected lane0=%d lane1=%d\n", s0, s1)
		fmt.Printf("  refI64=%d refF32=%f kernel=%f dispatch=%f\n", refI64, refF32, got, gotDD)
		
		if got != refF32 {
			t.Errorf("dim=%d: kernel %f != ref %f (diff=%f)", d, got, refF32, float64(got)-float64(refF32))
		}
	}
}
