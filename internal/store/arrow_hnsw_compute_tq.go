package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

type TurboQuantCompute struct {
	data    *types.GraphData
	encoder *TurboQuantEncoder
}

func NewTurboQuantCompute(data *types.GraphData) *TurboQuantCompute {
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
	return simd.L2SquaredFloat32(vec1, vec2)
}

func (c *TurboQuantCompute) DistanceWithVector(id uint32, vec []float32) (float32, error) {
	vec1, err := c.getVector(id)
	if err != nil {
		return 0, err
	}

	rotatedQuery := make([]float32, c.encoder.pow2)
	copy(rotatedQuery, vec)
	if err := simd.RandomRotation(rotatedQuery, c.encoder.params.Seed); err != nil {
		return 0, err
	}

	return simd.L2SquaredFloat32(vec1, rotatedQuery)
}

func (c *TurboQuantCompute) DistanceWithRotatedQuery(id uint32, rotatedQuery []float32) (float32, error) {
	vec1, err := c.getVector(id)
	if err != nil {
		return 0, err
	}
	return simd.L2SquaredFloat32(vec1, rotatedQuery)
}

func (c *TurboQuantCompute) PrecomputeRotatedQuery(vec []float32, output []float32) error {
	if len(output) < c.encoder.pow2 {
		output = make([]float32, c.encoder.pow2)
	}
	copy(output, vec)
	return simd.RandomRotation(output, c.encoder.params.Seed)
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

	return c.encoder.Decode(chunk[start : start+stride])
}
