//go:build !darwin || !arm64 || !cgo

package amx

// DotAMX runs the dot amx operation.
func DotAMX(a, b []float32) (float32, error) {
	return 0, nil
}

// L2AMX runs the l2 amx operation.
func L2AMX(a, b []float32) (float32, error) {
	return 0, nil
}
