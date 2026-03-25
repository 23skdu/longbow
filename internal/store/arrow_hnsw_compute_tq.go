package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// TurboQuantCompute implements the distance computation for TurboQuant vectors.
type TurboQuantCompute struct {
	data    *types.GraphData
	encoder *TurboQuantEncoder
}

func NewTurboQuantCompute(data *types.GraphData) *TurboQuantCompute {
	// Reconstruct encoder from GraphData params
	// Note: We need to store Seed in GraphData too, for now we'll assume 42 or wait for metadata
	encoder := NewTurboQuantEncoder(data.Dims, data.TurboQuantBits, 42)
	return &TurboQuantCompute{
		data:    data,
		encoder: encoder,
	}
}

func (c *TurboQuantCompute) Distance(id1, id2 uint32) (float32, error) {
	vec1, err := c.getVector(id1)
	if err != nil {
		return 0, err
	}
	vec2, err := c.getVector(id2)
	if err != nil {
		return 0, err
	}

	// For TurboQuant, we use the distance function assigned to the index (usually Euclidean)
	// After rotation, Euclidean distance is preserved.
	return simd.L2SquaredFloat32(vec1, vec2)
}

func (c *TurboQuantCompute) DistanceWithVector(id uint32, vec []float32) (float32, error) {
	vec1, err := c.getVector(id)
	if err != nil {
		return 0, err
	}

	// Rotate the query vector too!
	rotatedQuery := make([]float32, c.encoder.pow2)
	copy(rotatedQuery, vec)
	if err := simd.RandomRotation(rotatedQuery, c.encoder.params.Seed); err != nil {
		return 0, err
	}

	return simd.L2SquaredFloat32(vec1, rotatedQuery)
}

func (c *TurboQuantCompute) getVector(id uint32) ([]float32, error) {
	chunkID := int(id / uint32(types.ChunkSize))
	offset := int(id % uint32(types.ChunkSize))

	chunk := c.data.GetVectorsTQChunk(chunkID)
	if chunk == nil {
		return nil, fmt.Errorf("tq chunk %d not found", chunkID)
	}

	stride := 4 + (c.data.Dims-1)*c.data.TurboQuantBits/8 + (c.data.Dims+7)/8
	start := offset * stride
	if start+stride > len(chunk) {
		return nil, fmt.Errorf("tq offset out of bounds")
	}

	// Decode TQ bytes to rotated float32 vector
	return c.encoder.Decode(chunk[start : start+stride])
}
