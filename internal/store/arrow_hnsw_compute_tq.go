package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/simd"
)

type TurboQuantCompute struct {
	h       *ArrowHNSW
	encoder *TurboQuantEncoder
}

func NewTurboQuantCompute(h *ArrowHNSW) *TurboQuantCompute {
	data := h.data.Load()
	encoder := NewTurboQuantEncoder(data.Dims, data.TurboQuantBits, 42)
	return &TurboQuantCompute{
		h:       h,
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
	cID := chunkID(id)
	cOff := chunkOffset(id)
	data := c.h.data.Load()
	chunk := data.GetVectorsTQChunk(cID)
	if chunk == nil {
		return nil, fmt.Errorf("tq chunk %d not found", cID)
	}
	stride := PackedSize(int(data.Dims), data.TurboQuantBits)
	start := cOff * stride
	if start+stride > len(chunk) {
		return nil, fmt.Errorf("tq offset out of bounds")
	}
	return c.encoder.Decode(chunk[start : start+stride])
}
