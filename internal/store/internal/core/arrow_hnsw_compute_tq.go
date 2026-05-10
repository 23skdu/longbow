package core

import (
	"fmt"
	"math"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// TurboQuantCompute handles distance computations using TurboQuant encoding.
type TurboQuantCompute struct {
	h       *ArrowHNSW
	encoder *TurboQuantEncoder
}

// NewTurboQuantCompute creates a new TurboQuantCompute instance for the given HNSW index.
func NewTurboQuantCompute(h *ArrowHNSW) *TurboQuantCompute {
	data := h.data.Load()
	encoder := NewTurboQuantEncoder(data.Dims, data.TurboQuantBits, 42)
	return &TurboQuantCompute{
		h:       h,
		encoder: encoder,
	}
}

// Distance computes the distance between two vectors by their IDs using TurboQuant.
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

// DistanceWithVector computes the distance between a vector ID and a raw vector using TurboQuant.
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

// DistanceWithRotatedQuery computes the distance between a vector ID and a pre-rotated query vector.
func (c *TurboQuantCompute) DistanceWithRotatedQuery(id uint32, rotatedQuery []float32) (float32, error) {
	return c.DistanceWithRotatedQueryAndDisk(id, rotatedQuery, nil, math.MaxUint64)
}

// DistanceWithRotatedQueryAndDisk computes the distance using a pre-rotated query, allowing fallback to a DiskGraph.
func (c *TurboQuantCompute) DistanceWithRotatedQueryAndDisk(id uint32, rotatedQuery []float32, dg *DiskGraph, maxGen uint64) (float32, error) {
	vec1, err := c.getVectorWithDisk(id, dg, maxGen)
	if err != nil {
		return 0, err
	}
	return c.h.distFunc(vec1, rotatedQuery)
}

// PrecomputeRotatedQuery applies the random rotation to a query vector for faster subsequent searches.
func (c *TurboQuantCompute) PrecomputeRotatedQuery(vec []float32, output []float32) error {
	if len(output) < c.encoder.pow2 {
		output = make([]float32, c.encoder.pow2)
	}
	copy(output, vec)
	return simd.RandomRotation(output, c.encoder.params.Seed)
}

func (c *TurboQuantCompute) getVector(id uint32) ([]float32, error) {
	return c.getVectorWithDisk(id, nil, math.MaxUint64)
}

func (c *TurboQuantCompute) getVectorWithDisk(id uint32, dg *DiskGraph, maxGen uint64) ([]float32, error) {
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	data := c.h.data.Load()
	chunk := data.GetVectorsTQChunkWithGen(int(cID), maxGen)

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
// GetRadius extracts the radius information for a TurboQuant encoded vector.
func (c *TurboQuantCompute) GetRadius(id uint32, dg *DiskGraph, maxGen uint64) (float32, error) {
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	data := c.h.data.Load()
	chunk := data.GetVectorsTQChunkWithGen(int(cID), maxGen)

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
		if dg == nil {
			dg = c.h.diskGraph.Load()
		}
		if dg != nil {
			tqCode = dg.GetVectorTQ(id)
		}
	}

	if tqCode == nil {
		return 0, fmt.Errorf("tq vector %d not found for radius extraction", id)
	}

	return c.encoder.GetRadius(tqCode), nil
}
