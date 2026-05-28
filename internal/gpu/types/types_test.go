package types

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGPUBackendString(t *testing.T) {
	assert.Equal(t, "CPU", BackendCPU.String())
	assert.Equal(t, "CUDA", BackendCUDA.String())
	assert.Equal(t, "Metal", BackendMetal.String())
	assert.Equal(t, "TPU", BackendTPU.String())
	assert.Equal(t, "Unknown", GPUBackend(99).String())
}

func TestDefaultGPUConfig(t *testing.T) {
	t.Setenv("LONGBOW_MAX_MEMORY", "8589934592")
	t.Setenv("CUDA_HOME", "/usr/local/cuda")
	t.Setenv("FAISS_HOME", "/usr/local/faiss")
	cfg := DefaultGPUConfig()
	assert.Equal(t, BackendCPU, cfg.Backend)
	assert.Equal(t, int32(0), cfg.DeviceID)
	assert.Equal(t, 128, cfg.Dimension)
	assert.False(t, cfg.Enabled)
	assert.Equal(t, 1000, cfg.SyncBatchSize)
	assert.NotZero(t, cfg.SyncInterval)
	assert.Equal(t, int64(8589934592), cfg.MaxMemory)
	assert.Equal(t, "/usr/local/cuda", cfg.CUDAHome)
	assert.Equal(t, "/usr/local/faiss", cfg.FAISSHome)
}

func TestDefaultGPUConfigNoEnv(t *testing.T) {
	cfg := DefaultGPUConfig()
	assert.Equal(t, int64(0), cfg.MaxMemory)
}

func TestDetectGPUBackend(t *testing.T) {
	backend := DetectGPUBackend()
	assert.NotNil(t, backend)
}

func TestGetDeviceCount(t *testing.T) {
	count := GetDeviceCount()
	assert.GreaterOrEqual(t, count, int32(0))
}

func TestGetGlobalGPUUtilization(t *testing.T) {
	util, err := GetGlobalGPUUtilization()
	assert.NoError(t, err)
	assert.Equal(t, float32(0), util)
}

func TestGPUNotAvailableError(t *testing.T) {
	err := &GPUNotAvailableError{Reason: "no GPU found"}
	assert.Contains(t, err.Error(), "GPU not available")
	assert.Contains(t, err.Error(), "no GPU found")
	assert.True(t, IsGPUNotAvailableError(err))
	assert.False(t, IsGPUNotAvailableError(errors.New("other error")))
}

func TestGPUMemoryError(t *testing.T) {
	err := &GPUMemoryError{Requested: 1024, Available: 512, DeviceID: 0}
	assert.Contains(t, err.Error(), "GPU memory error")
	assert.Contains(t, err.Error(), "1024")
	assert.True(t, IsGPUMemoryError(err))
}

func TestGPUInitializationError(t *testing.T) {
	cause := errors.New("driver not found")
	err := &GPUInitializationError{DeviceID: 1, Backend: BackendCUDA, Cause: cause}
	assert.Contains(t, err.Error(), "GPU initialization failed")
	assert.Equal(t, cause, err.Unwrap())
	assert.True(t, IsGPUInitializationError(err))
}

func TestGPUComputeError(t *testing.T) {
	cause := errors.New("kernel crash")
	err := &GPUComputeError{Operation: "conv2d", DeviceID: 0, Cause: cause}
	assert.Contains(t, err.Error(), "GPU computation error")
	assert.Equal(t, cause, err.Unwrap())
	assert.True(t, IsGPUComputeError(err))
}

func TestGPUSyncError(t *testing.T) {
	cause := errors.New("sync failed")
	err := &GPUSyncError{BatchSize: 64, DeviceID: 0, Cause: cause}
	assert.Contains(t, err.Error(), "GPU sync error")
	assert.Equal(t, cause, err.Unwrap())
	assert.True(t, IsGPUSyncError(err))
}

func TestIsGPUError(t *testing.T) {
	assert.True(t, IsGPUError(&GPUNotAvailableError{}))
	assert.True(t, IsGPUError(&GPUMemoryError{}))
	assert.True(t, IsGPUError(&GPUInitializationError{}))
	assert.True(t, IsGPUError(&GPUComputeError{}))
	assert.True(t, IsGPUError(&GPUSyncError{}))
	assert.False(t, IsGPUError(errors.New("generic")))
}

func TestIsRetriableGPUError(t *testing.T) {
	assert.True(t, IsRetriableGPUError(&GPUMemoryError{}))
	assert.True(t, IsRetriableGPUError(&GPUSyncError{}))
	assert.True(t, IsRetriableGPUError(&GPUComputeError{}))
	assert.False(t, IsRetriableGPUError(&GPUNotAvailableError{}))
	assert.False(t, IsRetriableGPUError(&GPUInitializationError{}))
	assert.False(t, IsRetriableGPUError(errors.New("generic")))
}
