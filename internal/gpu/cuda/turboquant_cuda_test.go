//go:build gpu && linux && cuda

package cuda

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCUDAIndex_TurboQuantSearch(t *testing.T) {
	dim := 128
	count := 1000
	pow2 := 16 // Number of levels
	bitsPerAngle := 8

	config := types.GPUConfig{
		DeviceID:  0,
		MaxMemory: 1024 * 1024 * 1024,
		Dimension: dim,
	}

	idx, err := NewCUDAIndex(config)
	require.NoError(t, err)
	defer idx.Close()

	// Add vectors
	ids := make([]int64, count)
	// Calculate size of turboquant compressed representation
	// stride = 4 (radius) + angleBytes + bitBytes
	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	bitBytes := (pow2 + 7) / 8
	stride := 4 + angleBytes + bitBytes

	tqData := make([]byte, count*stride)

	for i := 0; i < count; i++ {
		ids[i] = int64(i)
		for j := 0; j < stride; j++ {
			tqData[i*stride+j] = byte(rand.Intn(256))
		}
	}

	err = idx.AddTurboQuant(ids, tqData, bitsPerAngle)
	require.NoError(t, err)
	err = idx.Flush()
	require.NoError(t, err)

	query := make([]float32, dim)
	for i := 0; i < dim; i++ {
		query[i] = rand.Float32()
	}

	results, distances, err := idx.SearchTurboQuant(query, 10, bitsPerAngle)
	require.NoError(t, err)

	assert.NotEmpty(t, results)
	assert.Equal(t, 10, len(results))
	assert.Equal(t, 10, len(distances))

	fmt.Printf("TurboQuant search found %d results\n", len(results))
}

func FuzzCUDAIndex_TurboQuantSearch(f *testing.F) {
	f.Add(128, 100, 16, 8)
	f.Fuzz(func(t *testing.T, dim, count, pow2, bitsPerAngle int) {
		if dim <= 0 || dim > 2048 || count <= 0 || count > 5000 {
			t.Skip()
		}
		if pow2 <= 0 || pow2 > 256 || bitsPerAngle <= 0 || bitsPerAngle > 16 {
			t.Skip()
		}

		config := types.GPUConfig{
			DeviceID:  0,
			MaxMemory: 1024 * 1024 * 1024,
			Dimension: dim,
		}

		idx, err := NewCUDAIndex(config)
		if err != nil {
			t.Skip()
		}
		defer idx.Close()

		ids := make([]int64, count)
		angleCount := pow2 - 1
		angleBytes := (angleCount*bitsPerAngle + 7) / 8
		bitBytes := (pow2 + 7) / 8
		stride := 4 + angleBytes + bitBytes
		tqData := make([]byte, count*stride)

		for i := 0; i < count; i++ {
			ids[i] = int64(i)
		}

		err = idx.AddTurboQuant(ids, tqData, bitsPerAngle)
		require.NoError(t, err)
		err = idx.Flush()
		require.NoError(t, err)

		query := make([]float32, dim)
		res, dists, err := idx.SearchTurboQuant(query, 10, bitsPerAngle)
		require.NoError(t, err)
		assert.Equal(t, len(res), len(dists))
	})
}
