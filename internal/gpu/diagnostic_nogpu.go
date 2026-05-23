//go:build !gpu

package gpu

import (
	"runtime"
	
	"github.com/rs/zerolog"
)

// CheckBinaryDiagnostic verifies if the binary is built with GPU support.
// Since this file is built without the 'gpu' tag, it checks hardware and warns if GPU capability is wasted.
func CheckBinaryDiagnostic(logger *zerolog.Logger) {
	hasGPU := false

	// Check if this hardware actually supports a GPU
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		hasGPU = true // Apple Silicon has Metal
	} else if runtime.GOOS == "linux" {
		// Just a quick check for nvidia devices or TPUs
		if len(DetectAvailableGPUs()) > 0 {
			hasGPU = true
		}
	}

	if hasGPU {
		logger.Warn().Msg("WARNING: Running CPU-only binary on GPU-capable hardware. The binary was not compiled with the 'gpu' build tag. Rebuild with 'make build-gpu' or 'make build-darwin-universal' for hardware acceleration.")
	} else {
		logger.Debug().Msg("Binary is built without GPU support, and no GPU hardware was detected. This is optimal for CPU-only nodes.")
	}
}
