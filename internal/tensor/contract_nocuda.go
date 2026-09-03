//go:build !gpu || !linux

package tensor

func contractCUDA(a, b, out *Tensor, aAxes, bAxes, aFree, bFree []int) bool {
	return false
}
