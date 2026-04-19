package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"math"
)

// int16Computer handles Int16 vectors
type int16Computer struct {
	data *types.GraphData
	q    []int16
	dims int
	h    *ArrowHNSW
}

func (c *int16Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *int16Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt16Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.h.distFuncInt16(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

// uint16Computer handles Uint16 vectors
type uint16Computer struct {
	data *types.GraphData
	q    []uint16
	dims int
	h    *ArrowHNSW
}

func (c *uint16Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *uint16Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint16Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.h.distFuncUint16(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

// int32Computer handles Int32 vectors
type int32Computer struct {
	data *types.GraphData
	q    []int32
	dims int
	h    *ArrowHNSW
}

func (c *int32Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *int32Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt32Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.h.distFuncInt32(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

// uint32Computer handles Uint32 vectors
type uint32Computer struct {
	data *types.GraphData
	q    []uint32
	dims int
	h    *ArrowHNSW
}

func (c *uint32Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *uint32Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint32Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.h.distFuncUint32(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

// int64Computer handles Int64 vectors
type int64Computer struct {
	data *types.GraphData
	q    []int64
	dims int
	h    *ArrowHNSW
}

func (c *int64Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *int64Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.h.distFuncInt64(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

// uint64Computer handles Uint64 vectors
type uint64Computer struct {
	data *types.GraphData
	q    []uint64
	dims int
	h    *ArrowHNSW
}

func (c *uint64Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *uint64Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.h.distFuncUint64(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}
