package pq

import (
	"encoding/binary"
	"errors"
	"math"
)

// Serialize converts the PQ encoder into a byte slice for persistence.
// Format:
// [4 bytes] Dims
// [4 bytes] M
// [4 bytes] K
// [M * K * SubDim * 4 bytes] Flattened Codebooks (float32)
func (e *PQEncoder) Serialize() []byte {
	subDim := e.SubDim
	size := 12 + (e.M * e.K * subDim * 4)
	data := make([]byte, size)

	binary.LittleEndian.PutUint32(data[0:], uint32(e.Dims))
	binary.LittleEndian.PutUint32(data[4:], uint32(e.M))
	binary.LittleEndian.PutUint32(data[8:], uint32(e.K))

	offset := 12
	for i := 0; i < e.M; i++ {
		cb := e.Codebooks[i]
		for _, val := range cb {
			bits := math.Float32bits(val)
			binary.LittleEndian.PutUint32(data[offset:], bits)
			offset += 4
		}
	}

	return data
}

// DeserializePQEncoder reconstructs a PQ encoder from a byte slice.
func DeserializePQEncoder(data []byte) (*PQEncoder, error) {
	if len(data) < 12 {
		return nil, errors.New("invalid PQ data: too short")
	}

	dims := int(binary.LittleEndian.Uint32(data[0:]))
	m := int(binary.LittleEndian.Uint32(data[4:]))
	k := int(binary.LittleEndian.Uint32(data[8:]))

	if m == 0 || dims%m != 0 {
		return nil, errors.New("invalid PQ parameters in serialized data")
	}

	subDim := dims / m
	expectedSize := 12 + (m * k * subDim * 4)
	if len(data) != expectedSize {
		return nil, errors.New("invalid PQ data: size mismatch")
	}

	encoder, err := NewPQEncoder(dims, m, k)
	if err != nil {
		return nil, err
	}

	offset := 12
	for i := 0; i < m; i++ {
		cb := encoder.Codebooks[i]
		for j := range cb {
			bits := binary.LittleEndian.Uint32(data[offset:])
			cb[j] = math.Float32frombits(bits)
			offset += 4
		}
	}

	return encoder, nil
}
