package cache

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"

	qry "github.com/23skdu/longbow/internal/query"
)

// HashQuery computes a unique 64-bit hash for a VectorSearchRequest.
// It considers all fields that affect the search result:
// - Dataset
// - Vector (elements)
// - K
// - Filters (recursively)
// - TextQuery (Hybrid)
// - Alpha
// - GraphAlpha
// - LocalOnly (maybe? usually same result regardless of where it runs, but context matters)
func HashQuery(req *qry.VectorSearchRequest) uint64 {
	h := fnv.New64a()

	// Dataset
	h.Write([]byte(req.Dataset))

	// Vector
	// Optimization: writing byte slice or elements?
	// FNV write is cheap.
	for _, v := range req.Vector {
		bits := math.Float32bits(v)
		binary.Write(h, binary.LittleEndian, bits)
	}

	// K
	binary.Write(h, binary.LittleEndian, int64(req.K))

	// TextQuery
	h.Write([]byte(req.TextQuery))

	// Alpha
	binary.Write(h, binary.LittleEndian, math.Float32bits(req.Alpha))

	// GraphAlpha
	binary.Write(h, binary.LittleEndian, math.Float32bits(req.GraphAlpha))

	// Filters
	if len(req.Filters) > 0 {
		hashFilters(h, req.Filters)
	}

	// LocalOnly? If local vs global changes result set (it might), then yes.
	if req.LocalOnly {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	return h.Sum64()
}

func hashFilters(h hash.Hash64, filters []qry.Filter) {
	for _, f := range filters {
		h.Write([]byte(f.Field))
		h.Write([]byte(f.Operator))
		h.Write([]byte(f.Value))
		h.Write([]byte(f.Logic))
		if len(f.Filters) > 0 {
			hashFilters(h, f.Filters)
		}
	}
}
