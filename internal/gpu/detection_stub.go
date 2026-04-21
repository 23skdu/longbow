//go:build !darwin
package gpu

func detectMetalGPULive() []GPUInfo {
	return nil
}
