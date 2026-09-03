//go:build !gpu || !linux

package tensor

func contractCUDA(_, _, _ *Tensor, _, _, _, _ []int) bool {
	return false
}
