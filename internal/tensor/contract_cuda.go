package tensor

import (
	"github.com/23skdu/longbow/internal/tensor/cuda"
)

// contractCUDA performs tensor contraction on the GPU using cuBLAS.
// Returns true if the contraction was offloaded.
func contractCUDA(a, b, out *Tensor, aAxes, bAxes, aFree, bFree []int) bool {
	if (a.Dtype() != DtypeFloat32 && a.Dtype() != DtypeFloat64) ||
		a.Dtype() != b.Dtype() || a.Dtype() != out.Dtype() {
		return false
	}
	// Only handle 2D contraction (matrix multiply) via cuBLAS for now
	if len(aFree) != 1 || len(bFree) != 1 || len(aAxes) != 1 {
		return false
	}
	m := a.Shape()[aFree[0]]
	n := b.Shape()[bFree[0]]
	k := a.Shape()[aAxes[0]]

	isFloat64 := (a.Dtype() == DtypeFloat64)
	if ok := cuda.ContractCUDA(a.Data(), b.Data(), out.Data(), isFloat64, m, n, k); ok {
		TensorOperationsTotal.WithLabelValues("contract", "cuda", a.Dtype().String()).Inc()
		return true
	}
	return false
}
