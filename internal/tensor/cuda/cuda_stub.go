//go:build !gpu || !linux

package cuda

// ContractCUDA is the fallback stub when compiling without GPU support.
func ContractCUDA(_, _, _ []byte, _ bool, _, _, _ int) bool {
	return false
}
