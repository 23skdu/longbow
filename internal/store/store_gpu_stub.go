//go:build !gpu

package store

import (
	"github.com/23skdu/longbow/internal/gpu"
)

// InitGPUBackend is a no-op for CPU-only builds
func (s *VectorStore) InitGPUBackend(backend gpu.GPUBackend, deviceID int) error {
	// GPU support not compiled in
	return nil
}

// initGPUIfEnabled is a no-op for CPU-only builds
func (s *VectorStore) initGPUIfEnabled(idx VectorIndex) {
	// GPU support not compiled in
}

// GetGPUMemoryStats returns zeros for CPU-only builds
func (s *VectorStore) GetGPUMemoryStats() (total, used, free int64) {
	return 0, 0, 0
}

// UpdateGPUMemoryMetrics is a no-op for CPU-only builds
func (s *VectorStore) UpdateGPUMemoryMetrics() {
	// GPU support not compiled in
}

// IsGPUEnabled returns false for CPU-only builds
func (s *VectorStore) IsGPUEnabled() bool {
	return false
}

// GetGPUBackend returns CPU for CPU-only builds
func (s *VectorStore) GetGPUBackend() gpu.GPUBackend {
	return gpu.BackendCPU
}
