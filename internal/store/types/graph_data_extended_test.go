package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphData_VariousVectorTypes(t *testing.T) {
	capacity := 10
	dims := 128
	
	// Test SQ8 - use Int8 type as base
	// quantization=true, sq8=true
	g := NewGraphData(capacity, dims, false, false, -1, true, true, false, VectorTypeInt8, false, false, false, 8)
	require.NotNil(t, g)
	
	err := g.EnsureChunk(0, 0, dims)
	assert.NoError(t, err)
	
	// Test SQ8 Get/Set
	sq8Data := make([]byte, dims)
	for i := range sq8Data {
		sq8Data[i] = byte(i % 256)
	}
	
	chunk := g.GetVectorsSQ8Chunk(0)
	assert.NotNil(t, chunk)
	copy(chunk[0:dims], sq8Data)
	
	back := g.GetVectorSQ8(0)
	assert.Equal(t, sq8Data, back)

	// Test BQ
	// quantization=true, bqEnabled=true
	gBQ := NewGraphData(capacity, dims, false, false, -1, true, false, false, VectorTypeBQ, true, false, false, 8)
	err = gBQ.EnsureChunk(0, 0, dims)
	assert.NoError(t, err)
	
	bqData := []uint64{0xAAAAAAAAAAAAAAAA, 0xBBBBBBBBBBBBBBBB}
	err = gBQ.SetVectorBQ(0, bqData)
	assert.NoError(t, err)
	
	backBQ, err := gBQ.GetVectorBQ(0)
	assert.NoError(t, err)
	assert.Equal(t, bqData, backBQ)
}

func TestGraphData_Metadata(t *testing.T) {
	g := NewGraphData(10, 4, true, true, 0, false, false, false, VectorTypeFloat32, false, false, false, 8)
	err := g.EnsureChunk(0, 0, 4)
	assert.NoError(t, err)
	
	// Test Lock/Unlock Node
	v := g.LockNode(0, 0)
	g.UnlockNode(0, 0, v)
	
	v2, ok := g.TryLockNode(0, 0)
	assert.True(t, ok)
	g.UnlockNode(0, 0, v2)
	
	// Test Metadata Resizing
	g.GrowMetadataSlices(20)
}

func TestGraphData_Clone(t *testing.T) {
	g := NewGraphData(10, 4, false, false, 0, false, false, false, VectorTypeFloat32, false, false, false, 8)
	_ = g.EnsureChunk(0, 0, 4)
	_ = g.SetVector(0, []float32{1, 2, 3, 4})
	
	g2 := g.Clone()
	require.NotNil(t, g2)
	assert.Equal(t, g.Dims, g2.Dims)
	
	v, _ := g2.GetVector(0)
	assert.Equal(t, []float32{1, 2, 3, 4}, v)
}
