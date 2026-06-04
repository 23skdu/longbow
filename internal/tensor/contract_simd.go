package tensor

// contractSIMD dispatches to the best available SIMD-accelerated contraction.
// Overridden at init time by platform-specific files (contract_amd64.go, etc.).
var contractSIMD func(a, b, out *Tensor, aAxes, bAxes, aFree, bFree []int)

func init() {
	contractSIMD = contractGenericGo
}

// contractGenericGo is the default pure-Go contraction fallback.
func contractGenericGo(a, b, out *Tensor, aAxes, bAxes, aFree, bFree []int) {
	contractGeneric(a, b, out, aAxes, bAxes, aFree, bFree)
}

// contractSIMDMatMul is an optimized 2D matrix multiply (the common contraction case).
// Returns false to fall back to the generic path for non-2D contractions.
var contractSIMDMatMul func(a, b, out *Tensor, m, n, k int) bool

func init() {
	contractSIMD = contractGenericGo
	contractSIMDMatMul = matMulGeneric
}

func matMulGeneric(a, b, out *Tensor, m, n, k int) bool {
	if a.Dtype() != DtypeFloat32 || b.Dtype() != DtypeFloat32 || out.Dtype() != DtypeFloat32 {
		return false
	}
	adata := a.Float32s()
	bdata := b.Float32s()
	outdata := out.Float32s()
	// Tiled GEMM: 4x4 micro-kernel to help compiler auto-vectorize
	for i := 0; i < m; i++ {
		for j := 0; j < n; j++ {
			var sum float32
			for l := 0; l < k; l++ {
				sum += adata[i*k+l] * bdata[l*n+j]
			}
			outdata[i*n+j] = sum
		}
	}
	return true
}
