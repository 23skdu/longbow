package types

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphData_FastPathAccessors(t *testing.T) {
	dims := 128
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-fast", nil, false)
	require.NotNil(t, g)

	err := g.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	chunkFast := g.GetVectorsChunkFast(0)
	chunkOrig := g.GetVectorsChunkWithGen(0, math.MaxUint64)
	assert.Equal(t, chunkOrig, chunkFast, "fast path should return same data as original")

	chunkFastGen := g.GetVectorsChunkFastWithGen(0, math.MaxUint64)
	assert.Equal(t, chunkOrig, chunkFastGen, "fast with gen should return same data")
}

func TestGraphData_FastPathNeighborsCountsVersions(t *testing.T) {
	dims := 128
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-ncv", nil, false)
	require.NotNil(t, g)

	err := g.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	fast := g.GetNeighborsChunkFast(0, 0)
	orig := g.GetNeighborsChunkWithGen(0, 0, math.MaxUint64)
	assert.Equal(t, orig, fast, "neighbors fast path parity")

	countsFast := g.GetCountsChunkFast(0, 0)
	countsOrig := g.GetCountsChunkWithGen(0, 0, math.MaxUint64)
	assert.Equal(t, countsOrig, countsFast, "counts fast path parity")

	verFast := g.GetVersionsChunkFast(0, 0)
	verOrig := g.GetVersionsChunkWithGen(0, 0, math.MaxUint64)
	assert.Equal(t, verOrig, verFast, "versions fast path parity")
}

func TestGraphData_FastPathInt8Types(t *testing.T) {
	dims := 128
	g := NewGraphData(10, dims, false, false, -1, true, false, false, VectorTypeInt8, false, false, false, 8, "test-int8", nil, false)
	require.NotNil(t, g)

	err := g.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	fast := g.GetVectorsInt8ChunkFast(0)
	orig := g.GetVectorsInt8ChunkWithGen(0, math.MaxUint64)
	assert.Equal(t, orig, fast, "int8 fast path parity")
}

func TestGraphData_FastPathInt16Types(t *testing.T) {
	dims := 128
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeInt16, false, false, false, 8, "test-int16", nil, false)
	require.NotNil(t, g)

	err := g.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	fast := g.GetVectorsInt16ChunkFast(0)
	orig := g.GetVectorsInt16ChunkWithGen(0, math.MaxUint64)
	assert.Equal(t, orig, fast, "int16 fast path parity")
}

func TestGraphData_GenerationIsolation(t *testing.T) {
	dims := 128
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-gen", nil, false)
	require.NotNil(t, g)

	err := g.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	// With MaxUint64, committed data is always visible
	data := g.GetVectorsChunkFastWithGen(0, math.MaxUint64)
	assert.NotNil(t, data, "should see data with MaxUint64")

	// With generation 0, data created by EnsureChunk should still be visible
	// since newly allocated slabs start at generation 1
	data = g.GetVectorsChunkFastWithGen(0, 0)
	assert.NotNil(t, data, "newly allocated chunk should be visible even at gen 0")
}

func TestGraphData_GetVectorPQWithGen(t *testing.T) {
	dims := 128
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-pqgen", nil, false)
	require.NotNil(t, g)

	err := g.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	// Without PQ enabled, GetVectorPQWithGen should return nil (not panic)
	code := g.GetVectorPQWithGen(0, math.MaxUint64)
	assert.Nil(t, code, "should return nil when PQ is not enabled")
}

func TestGraphData_FastPathConcurrentReadWrite(t *testing.T) {
	dims := 128
	g := NewGraphData(100, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-conc", nil, false)
	require.NotNil(t, g)

	err := g.EnsureChunk(0, 0, dims)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				chunk := g.GetVectorsChunkFast(0)
				if chunk != nil {
					_ = chunk[0]
				}
				chunk = g.GetVectorsChunkFastWithGen(0, math.MaxUint64)
				if chunk != nil {
					_ = chunk[0]
				}
			}
		}()
	}
	wg.Wait()
}
