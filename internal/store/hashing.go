package store

import (
	"encoding/binary"
	"math"

	"github.com/cespare/xxhash/v2"
)

// HashHybridQuery generates a deterministic hash for a hybrid search query.
// It combines dataset name, vector, text query, and parameters into a uint64 key.
func HashHybridQuery(dataset string, vector []float32, textQuery string, k int, alpha float32, rrfK int, graphAlpha float32, graphDepth int) uint64 {
	h := xxhash.New()

	// Dataset Name
	_, _ = h.WriteString(dataset)

	// Vector
	// Use unsafe/fast conversion if possible?
	// For now, simple loop is safe and reasonably fast for 128-dim.
	// We handle float32 bit representation.
	var buf [4]byte
	for _, v := range vector {
		bits := math.Float32bits(v)
		binary.LittleEndian.PutUint32(buf[:], bits)
		_, _ = h.Write(buf[:])
	}

	// Text Query
	_, _ = h.WriteString(textQuery)

	// Params
	// K
	var buf8 [8]byte
	binary.LittleEndian.PutUint64(buf8[:], uint64(k))
	_, _ = h.Write(buf8[:])

	// Alpha
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(alpha))
	_, _ = h.Write(buf[:])

	// RRF K
	binary.LittleEndian.PutUint64(buf8[:], uint64(rrfK))
	_, _ = h.Write(buf8[:])

	// Graph Alpha
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(graphAlpha))
	_, _ = h.Write(buf[:])

	// Graph Depth
	binary.LittleEndian.PutUint64(buf8[:], uint64(graphDepth))
	_, _ = h.Write(buf8[:])

	return h.Sum64()
}
