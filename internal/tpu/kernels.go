package tpu

import (
	"fmt"
	"os"
)

// LoadKernel implements Phase 2: Custom TPU Kernels & SIMD.
// It loads a pre-compiled High-Level Optimizer (HLO) payload generated via JAX/XLA from disk.
// These payloads are pre-structured to utilize the Matrix Multiply Unit (MXU) and Vector Processing Unit (VPU).
func (c *Client) LoadKernel(filename string) (Executable, error) {
	if filename == "" {
		return nil, fmt.Errorf("kernel filename cannot be empty")
	}

	payload, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read HLO kernel %s: %w", filename, err)
	}

	// Pass the payload to the PJRT C-API to compile an executable for the active TPU.
	return c.CompileHLO(payload)
}
