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

func TestTPUIndex_FeatureGaps(t *testing.T) {
	cfg := types.GPUConfig{DeviceID: 0, Dimension: 8}
	idx, err := NewTPUIndexImpl(cfg)
	require.NoError(t, err)
	defer idx.Close()

	// 1. HaversineSearch
	points := []float32{40.7128, -74.0060, 34.0522, -118.2437} // NY, LA
	centerLat, centerLon := float32(40.7128), float32(-74.0060)
	dists, err := idx.HaversineSearch(centerLat, centerLon, points, 6371.0)
	require.NoError(t, err)
	require.Len(t, dists, 2)
	require.InDelta(t, 0.0, dists[0], 0.1)
	require.True(t, dists[1] > 3000.0)

	// 2. NormBatch
	vectors := make([]float32, 8*2)
	vectors[0], vectors[1] = 3.0, 4.0
	vectors[8], vectors[9] = 1.0, 1.0
	norms, err := idx.NormBatch(vectors, 8)
	require.NoError(t, err)
	require.Len(t, norms, 2)
	require.InDelta(t, 5.0, norms[0], 0.001)

	// 3. PruneNeighbors
	candIds := []uint32{1, 2, 3}
	candDists := []float32{0.1, 0.5, 0.2}
	pruned, err := idx.PruneNeighbors(candIds, candDists, 2, nil)
	require.NoError(t, err)
	require.Len(t, pruned, 2)
	require.Equal(t, uint32(1), pruned[0])
	require.Equal(t, uint32(3), pruned[1])

	// 4. Complex Search (F16 simulation)
	f16Vec := make([]uint16, 8)
	f16Vec[0] = 0x3c00
	resIDs, _, err := idx.SearchComplex64(f16Vec, 1)
	require.NoError(t, err)
	require.NotNil(t, resIDs)

	f32Vec := make([]float32, 8)
	_, _, err = idx.SearchComplex128(f32Vec, 1)
	require.NoError(t, err)

	// 5. PQ Methods (Stubs)
	err = idx.AddPQ(nil, nil, 0)
	require.Error(t, err)
	_, _, err = idx.SearchPQ(nil, 0, 0)
	require.Error(t, err)
	err = idx.TrainPQ(nil, 0, 0)
	require.Error(t, err)
	_, err = idx.EncodePQ(nil)
	require.Error(t, err)

	// 6. Graph Methods
	err = idx.UpdateGraph([]uint32{0, 1}, []uint32{0}, nil)
	require.NoError(t, err)
	gids, weights, err := idx.GraphExpand([]uint32{0}, 1, 0.5)
	require.NoError(t, err)
	require.Len(t, gids, 1)
	require.Len(t, weights, 1)

	// 7. Utility Methods
	backend := idx.Backend()
	require.Equal(t, types.BackendTPU, backend)
	require.Equal(t, int32(0), idx.DeviceID())
	util, err := idx.GetUtilization()
	require.NoError(t, err)
	require.Equal(t, float32(0), util)

	// 8. SearchBatch
	_, _, err = idx.SearchBatch([][]float32{make([]float32, 8)}, 1)
	require.NoError(t, err)

	// 9. Reset and Clear
	err = idx.Reset()
	require.NoError(t, err)
	err = idx.Clear()
	require.NoError(t, err)

	// 10. TurboQuant
	// Dimension 8, 8 bits per angle. Needs 4 (radius) + 7 (angles) = 11 bytes minimum.
	tqData := make([]byte, 16)
	err = idx.AddTurboQuant([]int64{1}, tqData, 8)
	require.NoError(t, err)
	_, _, err = idx.SearchTurboQuant(make([]float32, 8), 1, 8)
	require.NoError(t, err)

	// 11. AssignToClusters
	assignments, err := idx.AssignToClusters(make([]float32, 8), make([]float32, 8))
	require.NoError(t, err)
	require.Len(t, assignments, 1)
}
