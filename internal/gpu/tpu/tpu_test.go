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

func TestTPUIndex_Functional(t *testing.T) {
	cfg := types.GPUConfig{DeviceID: 0, Dimension: 4}
	idx, err := NewTPUIndexImpl(cfg)
	require.NoError(t, err)
	defer idx.Close()

	// Test Add
	vectors := []float32{
		1.0, 0.0, 0.0, 0.0,
		0.0, 1.0, 0.0, 0.0,
	}
	ids := []int64{1, 2}
	err = idx.Add(ids, vectors)
	require.NoError(t, err)

	// Test Search
	query := []float32{1.0, 0.1, 0.0, 0.0}
	resIDs, dists, err := idx.Search(query, 2)
	require.NoError(t, err)
	require.Len(t, resIDs, 2)
	require.Equal(t, int64(1), resIDs[0])
	require.Equal(t, int64(2), resIDs[1])
	require.True(t, dists[0] < dists[1])
}
