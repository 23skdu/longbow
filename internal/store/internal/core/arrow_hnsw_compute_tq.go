package core

import (
	"github.com/23skdu/longbow/internal/store/types"
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
	return c.h.distFunc(vec1, vec2)
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

	return c.h.distFunc(vec1, rotatedQuery)
}

func (c *TurboQuantCompute) DistanceWithRotatedQuery(id uint32, rotatedQuery []float32) (float32, error) {
	return c.DistanceWithRotatedQueryAndDisk(id, rotatedQuery, nil)
}

func (c *TurboQuantCompute) DistanceWithRotatedQueryAndDisk(id uint32, rotatedQuery []float32, dg *DiskGraph) (float32, error) {
	vec1, err := c.getVectorWithDisk(id, dg)
	if err != nil {
		return 0, err
	}
	return c.h.distFunc(vec1, rotatedQuery)
}

func (c *TurboQuantCompute) PrecomputeRotatedQuery(vec []float32, output []float32) error {
	if len(output) < c.encoder.pow2 {
		output = make([]float32, c.encoder.pow2)
	}
	copy(output, vec)
	return simd.RandomRotation(output, c.encoder.params.Seed)
}

func (c *TurboQuantCompute) getVector(id uint32) ([]float32, error) {
	return c.getVectorWithDisk(id, nil)
}

func (c *TurboQuantCompute) getVectorWithDisk(id uint32, dg *DiskGraph) ([]float32, error) {
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	data := c.h.data.Load()
	chunk := data.GetVectorsTQChunk(cID)

	var tqCode []byte
	var stride int

	if chunk != nil {
		stride = PackedSize(int(data.Dims), data.TurboQuantBits)
		start := cOff * stride
		if start+stride <= len(chunk) {
			tqCode = chunk[start : start+stride]
		}
	}

	if tqCode == nil {
		// Fallback to DiskGraph
		if dg == nil {
			dg = c.h.diskGraph.Load()
		}
		if dg != nil {
			tqCode = dg.GetVectorTQ(id)
		}
	}

	if tqCode == nil {
		return nil, fmt.Errorf("tq vector %d not found", id)
	}

	return c.encoder.Decode(tqCode)
}
