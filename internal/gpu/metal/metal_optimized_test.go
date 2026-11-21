//go:build gpu && darwin && arm64

package metal

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetalIndexOptimized_Lifecycle(t *testing.T) {
	cfg := types.GPUConfig{
		DeviceID:  0,
		Dimension: 4,
	}
	idx, err := NewMetalIndexOptimized(cfg)
	if err != nil {
		t.Skipf("Metal GPU not available: %v", err)
	}
	defer idx.Close()

	// 1. Test Add
	vectors := []float32{
		1.0, 0.0, 0.0, 0.0,
		0.0, 1.0, 0.0, 0.0,
	}
	ids := []int64{1, 2}
	err = idx.Add(ids, vectors)
	require.NoError(t, err)

	// 2. Test Search
	query := []float32{1.0, 0.1, 0.0, 0.0}
	resIDs, _, err := idx.Search(query, 2)
	require.NoError(t, err)
	require.Len(t, resIDs, 2)
	require.Equal(t, int64(1), resIDs[0])

	// 3. Test SearchBatch
	batchResults, batchDists, err := idx.SearchBatch([][]float32{query}, 1)
	require.NoError(t, err)
	require.Len(t, batchResults, 1)
	require.Len(t, batchDists, 1)

	// 4. Test Utility methods
	require.Equal(t, types.BackendMetal, idx.Backend())
	require.Equal(t, int32(0), idx.DeviceID())
	info, err := idx.GetDeviceInfo()
	require.NoError(t, err)
	require.NotNil(t, info)
}

func TestMetalIndexOptimized_Functional(t *testing.T) {
	cfg := types.GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	}
	idx, err := NewMetalIndexOptimized(cfg)
	if err != nil {
		t.Skipf("Metal GPU not available: %v", err)
	}
	defer idx.Close()

	// 1. Test HaversineSearch
	points := []float32{40.7128, -74.0060, 34.0522, -118.2437} // NY, LA
	centerLat, centerLon := float32(40.7128), float32(-74.0060)
	dists, err := idx.HaversineSearch(centerLat, centerLon, points, 6371.0)
	require.NoError(t, err)
	require.Len(t, dists, 2)
	require.InDelta(t, 0.0, dists[0], 0.1)

	// 2. Test NormBatch
	vectors := make([]float32, 128*2)
	vectors[0], vectors[1] = 3.0, 4.0
	vectors[128], vectors[129] = 1.0, 1.0
	norms, err := idx.NormBatch(vectors, 128)
	require.NoError(t, err)
	require.Len(t, norms, 2)
	require.InDelta(t, 5.0, norms[0], 1e-4)
	require.InDelta(t, math.Sqrt(2.0), norms[1], 1e-4)

	// 3. Test Clear and Sync
	err = idx.Sync()
	require.NoError(t, err)
	err = idx.Clear()
	require.NoError(t, err)
}

func TestMetalIndexOptimized_Prune(t *testing.T) {
	cfg := types.GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	}
	idx, err := NewMetalIndexOptimized(cfg)
	if err != nil {
		t.Skipf("Metal GPU not available: %v", err)
	}
	defer idx.Close()

	candIds := []uint32{1, 2, 3}
	candDists := []float32{0.1, 0.5, 0.2}
	allVectors := make([]float32, 128*4) // 4 vectors
	// Make vectors distinct and orthogonal so they aren't pruned as redundant
	allVectors[1*128+0] = 1.0
	allVectors[2*128+1] = 1.0
	allVectors[3*128+2] = 1.0

	pruned, err := idx.PruneNeighbors(candIds, candDists, 2, allVectors)
	require.NoError(t, err)
	require.Len(t, pruned, 2)
}

func cpuL2Ref(a, b []float32) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return float32(math.Sqrt(sum))
}

func verifyAllDTypesOnIndex(t *testing.T, newIdx func(dim int) (types.Index, error)) {
	t.Helper()
	dimsList := []int{128, 384, 768}

	for _, dim := range dimsList {
		dim := dim
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			idx, err := newIdx(dim)
			if err != nil {
				t.Skipf("Metal GPU not available: %v", err)
			}
			defer idx.Close()

			const numVecs = 12
			rng := rand.New(rand.NewSource(int64(42 + dim)))
			vectors := make([]float32, numVecs*dim)
			ids := make([]int64, numVecs)
			for i := 0; i < numVecs; i++ {
				ids[i] = int64(i)
				for j := 0; j < dim; j++ {
					// Use small integer values so int8/uint8/int16/uint16/fp16 round-trip cleanly
					vectors[i*dim+j] = float32(rng.Intn(40) + 1)
				}
			}

			require.NoError(t, idx.Add(ids, vectors))

			targetIdx := 5
			targetVec := vectors[targetIdx*dim : (targetIdx+1)*dim]
			expectedOtherDist := cpuL2Ref(targetVec, vectors[0:dim])

			// Float32
			t.Run("float32", func(t *testing.T) {
				resIDs, resDists, err := idx.Search(targetVec, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-3)
				assert.Greater(t, resDists[1], float32(0.0))
				_ = expectedOtherDist
			})

			// Float16
			t.Run("float16", func(t *testing.T) {
				q := make([]uint16, dim)
				for j, v := range targetVec {
					q[j] = float16.New(v).Uint16()
				}
				resIDs, resDists, err := idx.SearchFloat16(q, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-2)
			})

			// Complex64 (interleaved fp16 pairs)
			t.Run("complex64", func(t *testing.T) {
				q := make([]uint16, dim)
				for j, v := range targetVec {
					q[j] = float16.New(v).Uint16()
				}
				resIDs, resDists, err := idx.SearchComplex64(q, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-2)
			})

			// Complex128 (interleaved float32 pairs)
			t.Run("complex128", func(t *testing.T) {
				resIDs, resDists, err := idx.SearchComplex128(targetVec, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-3)
			})

			// Int8
			t.Run("int8", func(t *testing.T) {
				q := make([]int8, dim)
				for j, v := range targetVec {
					q[j] = int8(v)
				}
				resIDs, resDists, err := idx.SearchInt8(q, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-3)
			})

			// Uint8
			t.Run("uint8", func(t *testing.T) {
				q := make([]uint8, dim)
				for j, v := range targetVec {
					q[j] = uint8(v)
				}
				resIDs, resDists, err := idx.SearchUint8(q, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-3)
			})

			// Int16
			t.Run("int16", func(t *testing.T) {
				q := make([]int16, dim)
				for j, v := range targetVec {
					q[j] = int16(v)
				}
				resIDs, resDists, err := idx.SearchInt16(q, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-3)
			})

			// Uint16
			t.Run("uint16", func(t *testing.T) {
				q := make([]uint16, dim)
				for j, v := range targetVec {
					q[j] = uint16(v)
				}
				resIDs, resDists, err := idx.SearchUint16(q, 3)
				require.NoError(t, err)
				require.Len(t, resIDs, 3)
				assert.Equal(t, int64(targetIdx), resIDs[0])
				assert.InDelta(t, 0.0, resDists[0], 1e-3)
			})

			// Distance accuracy check against CPU reference for all k results
			resIDs, resDists, err := idx.Search(targetVec, numVecs)
			require.NoError(t, err)
			require.Len(t, resIDs, numVecs)
			for rank, id := range resIDs {
				wantDist := cpuL2Ref(targetVec, vectors[int(id)*dim:(int(id)+1)*dim])
				assert.InDelta(t, wantDist, resDists[rank], 1e-2, "dim=%d rank=%d id=%d", dim, rank, id)
			}
		})
	}
}

func TestMetalIndexOptimized_AllDTypesAndDims(t *testing.T) {
	verifyAllDTypesOnIndex(t, func(dim int) (types.Index, error) {
		return NewMetalIndexOptimized(types.GPUConfig{DeviceID: 0, Dimension: dim})
	})
}

func TestMetalIndex_StandardAllDTypesAndDims(t *testing.T) {
	verifyAllDTypesOnIndex(t, func(dim int) (types.Index, error) {
		return NewMetalIndexImpl(types.GPUConfig{DeviceID: 0, Dimension: dim})
	})

	// Also verify standard MetalIndex Haversine, NormBatch, PruneNeighbors, and GraphExpand
	idx, err := NewMetalIndexImpl(types.GPUConfig{DeviceID: 0, Dimension: 128})
	if err != nil {
		t.Skipf("Metal GPU not available: %v", err)
	}
	defer idx.Close()

	points := []float32{40.7128, -74.0060, 34.0522, -118.2437}
	dists, err := idx.HaversineSearch(40.7128, -74.0060, points, 6371.0)
	require.NoError(t, err)
	require.Len(t, dists, 2)
	assert.InDelta(t, 0.0, dists[0], 0.1)
	assert.InDelta(t, 3935.7, dists[1], 10.0)

	vecs := make([]float32, 128*2)
	vecs[0], vecs[1] = 3.0, 4.0
	vecs[128], vecs[129] = 6.0, 8.0
	norms, err := idx.NormBatch(vecs, 128)
	require.NoError(t, err)
	require.Len(t, norms, 2)
	assert.InDelta(t, 5.0, norms[0], 1e-4)
	assert.InDelta(t, 10.0, norms[1], 1e-4)

	candIds := []uint32{1, 2, 3}
	candDists := []float32{0.1, 0.5, 0.2}
	allVecs := make([]float32, 128*4)
	allVecs[1*128+0] = 1.0
	allVecs[2*128+1] = 1.0
	allVecs[3*128+2] = 1.0
	pruned, err := idx.PruneNeighbors(candIds, candDists, 2, allVecs)
	require.NoError(t, err)
	require.Len(t, pruned, 2)

	// GraphExpand: 0 -> 1 -> 2
	offsets := []uint32{0, 1, 2, 2}
	neighbors := []uint32{1, 2}
	weights := []float32{1.0, 1.0}
	require.NoError(t, idx.UpdateGraph(offsets, neighbors, weights))
	outIDs, outScores, err := idx.GraphExpand([]uint32{0}, 1, 0.5)
	require.NoError(t, err)
	require.NotEmpty(t, outIDs)
	require.Equal(t, len(outIDs), len(outScores))
	assert.Equal(t, uint32(1), outIDs[0])
	assert.InDelta(t, 0.5, outScores[0], 1e-4)
}

func TestMetalHybridIndex_AllDTypesAndDims(t *testing.T) {
	verifyAllDTypesOnIndex(t, func(dim int) (types.Index, error) {
		return NewMetalHybridIndex(types.GPUConfig{DeviceID: 0, Dimension: dim})
	})
}
