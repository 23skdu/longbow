package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGraphData_Serialization(t *testing.T) {
	// Create sample GraphData
	capacity := 100
	dims := 4
	g := NewGraphData(capacity, dims, false, false, 0, false, false, false, VectorTypeFloat32, false, false)

	// Populate vectors
	for i := 0; i < 10; i++ {
		vec := []float32{float32(i), float32(i), float32(i), float32(i)}
		if err := g.EnsureChunk(i/ChunkSize, i%ChunkSize, dims); err != nil {
			t.Fatalf("ensure chunk failed: %v", err)
		}
		if err := g.SetVector(uint32(i), vec); err != nil {
			t.Fatalf("set vector failed: %v", err)
		}
	}

	// Populate neighbors
	// Node 0 -> [1, 2]
	// Node 1 -> [3]
	if err := g.SetNeighbors(0, []uint32{1, 2}); err != nil {
		t.Fatalf("set neighbors 0 failed: %v", err)
	}
	if err := g.SetNeighbors(1, []uint32{3}); err != nil {
		t.Fatalf("set neighbors 1 failed: %v", err)
	}

	// Serialize
	var buf bytes.Buffer
	if err := g.Serialize(&buf); err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	// Deserialize
	g2, err := DeserializeGraphData(&buf)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	// Verify Metadata
	assert.Equal(t, g.Capacity, g2.Capacity)
	assert.Equal(t, g.Dims, g2.Dims)
	assert.Equal(t, g.Type, g2.Type)

	// Verify Vectors
	for i := 0; i < 10; i++ {
		v1, _ := g.GetVector(uint32(i))
		v2, _ := g2.GetVector(uint32(i))
		assert.Equal(t, v1, v2, "vector mismatch at %d", i)
	}

	// Verify Neighbors
	n0_1 := g.GetNeighbors(0, 0, nil)
	n0_2 := g2.GetNeighbors(0, 0, nil)
	assert.Equal(t, n0_1, n0_2)

	n1_1 := g.GetNeighbors(0, 1, nil)
	n1_2 := g2.GetNeighbors(0, 1, nil)
	assert.Equal(t, n1_1, n1_2)
}
