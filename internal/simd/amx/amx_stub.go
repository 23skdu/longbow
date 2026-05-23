//go:build !darwin || !arm64

package amx

func DotAMX(a, b []float32) (float32, error) {
	return 0, nil
}

func L2AMX(a, b []float32) (float32, error) {
	return 0, nil
}
