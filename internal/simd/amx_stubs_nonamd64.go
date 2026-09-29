//go:build !amd64

package simd

import "github.com/apache/arrow-go/v18/arrow/float16"

// AMX is an x86 tile extension (Sapphire Rapids+) and has no ARM counterpart.
// dispatch.go builds the emerald/granite dispatch tables unconditionally, so the
// AMX entry points must resolve on every architecture even though detectCPU can
// never select those tables off x86. They map to the NEON kernels here, which in
// turn fall back to the portable unrolled implementations where NEON is absent.

func euclideanAMX(a, b []float32) (float32, error) { return euclideanNEON(a, b) }

func dotAMX(a, b []float32) (float32, error) { return dotNEON(a, b) }

func l2SquaredAMX(a, b []float32) (float32, error) { return l2SquaredNEON(a, b) }

func matMulAMX(a, b []float32, m, n, k int, dst []float32) { matMulNEON(a, b, m, n, k, dst) }

func euclideanBatchAMX(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchNEON(query, vectors, results)
}

func dotBatchAMX(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchNEON(query, vectors, results)
}

func euclideanF16AMX(a, b []float16.Num) (float32, error) { return euclideanF16NEON(a, b) }

func dotF16AMX(a, b []float16.Num) (float32, error) { return dotF16NEON(a, b) }
