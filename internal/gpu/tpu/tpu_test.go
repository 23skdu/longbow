package tpu

import (
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/stretchr/testify/require"
)

func TestTPUBackend_Initialize(t *testing.T) {
	backend, err := NewTPUBackend(0)
	require.NoError(t, err)
	require.NotNil(t, backend)

	err = backend.Initialize()
	require.NoError(t, err)
	require.True(t, backend.initialized)
}

func TestTPUBackend_DeviceInfo(t *testing.T) {
	backend, _ := NewTPUBackend(0)
	info, err := backend.GetDeviceInfo()
	require.NoError(t, err)
	require.Equal(t, types.BackendTPU, info.Backend)
	require.Contains(t, info.Name, "Ironwood")
}

func TestTPUIndex_GetMemoryInfo(t *testing.T) {
	cfg := types.GPUConfig{DeviceID: 0, Dimension: 128}
	idx, err := NewTPUIndexImpl(cfg)
	require.NoError(t, err)

	total, free, used, err := idx.GetMemoryInfo()
	require.NoError(t, err)
	require.Equal(t, int64(192*1024*1024*1024), total)
	require.True(t, free > 0)
	require.Equal(t, int64(0), used)
}

func TestTPU_EnqueueStub(t *testing.T) {
	data := make([]float32, 1024)
	err := tpuEnqueueBatch(0, data)
	require.NoError(t, err)
}
