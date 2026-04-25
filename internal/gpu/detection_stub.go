//go:build !darwin || !cgo
package gpu

func detectMetalGPULive() []GPUInfo {
	return nil
}
