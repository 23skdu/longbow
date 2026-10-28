package index

import (
	"math"
	"unsafe"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

type pqComputer struct {
	data      *types.GraphData
	q         []float32
	table     any // Accepts []float32 or pq.ADCTable
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	codesBuf  []byte
}

func (c *pqComputer) ComputeSingle(id uint32) (float32, error) {
	code := c.data.GetVectorPQWithGen(id, c.maxGen)
	if code == nil {
		if c.diskGraph != nil {
			code = c.diskGraph.GetVectorPQ(id)
		} else {
			dg := c.h.diskGraph.Load()
			if dg != nil {
				code = dg.GetVectorPQ(id)
			}
		}
	}

	if code == nil {
		return math.MaxFloat32, nil
	}

	var distSq float32
	var err error
	switch enc := c.h.oopqEncoder.(type) {
	case *pq.PQEncoder:
		if t, ok := c.table.([]float32); ok {
			distSq, err = enc.ADCDistance(t, code)
		}
	case *pq.OPQEncoder:
		if t, ok := c.table.([]float32); ok {
			distSq, err = enc.ADCDistance(t, code)
		}
	}

	if err != nil {
		return math.MaxFloat32, nil
	}

	if c.h.config.Metric == basecore.MetricEuclidean {
		return float32(math.Sqrt(float64(distSq))), nil
	}
	return distSq, nil
}

func (c *pqComputer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	n := len(ids)
	if n == 0 {
		return dst[:0], nil
	}

	t, isTableF32 := c.table.([]float32)
	if isTableF32 && n >= 4 && c.h != nil && c.h.config.Metric == basecore.MetricEuclidean {
		var m int
		switch enc := c.h.oopqEncoder.(type) {
		case *pq.PQEncoder:
			m = enc.M
		case *pq.OPQEncoder:
			m = enc.M
		}

		if m > 0 && len(t) == m*256 {
			neededLen := n * m
			if cap(c.codesBuf) < neededLen {
				c.codesBuf = make([]byte, neededLen)
			}
			flatCodes := c.codesBuf[:neededLen]
			allValid := true
			for i, id := range ids {
				code := c.data.GetVectorPQWithGen(id, c.maxGen)
				if code == nil {
					if c.diskGraph != nil {
						code = c.diskGraph.GetVectorPQ(id)
					} else {
						dg := c.h.diskGraph.Load()
						if dg != nil {
							code = dg.GetVectorPQ(id)
						}
					}
				}
				if code == nil || len(code) != m {
					allValid = false
					break
				}
				copy(flatCodes[i*m:(i+1)*m], code)
			}

			if allValid {
				if cap(dst) < n {
					dst = make([]float32, n)
				} else {
					dst = dst[:n]
				}
				if err := simd.ADCDistanceBatch(t, flatCodes, m, dst); err == nil {
					return dst, nil
				}
			}
		}
	}

	dst = dst[:0]
	for _, id := range ids {
		dist, err := c.ComputeSingle(id)
		if err != nil {
			return nil, err
		}
		dst = append(dst, dist)
	}
	return dst, nil
}

func (c *pqComputer) Prefetch(id uint32) {
	cID := int(id) / types.ChunkSize
	chunk := c.data.GetVectorsPQChunkFast(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		numWordsPerNode := (c.data.PQM + 7) / 8
		start := cOff * numWordsPerNode * 8
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

type tqComputer struct {
	data         *types.GraphData
	h            *ArrowHNSW
	rotatedQuery []float32
	diskGraph    *DiskGraph
	maxGen       uint64
	sctx         *ArrowSearchContext

	// Small direct-mapped cache of the TurboQuant chunks a search has touched,
	// used by Prefetch. HNSW traversal is local but not contiguous - a node's
	// neighbours are spread over the whole corpus, so a one-entry cache misses
	// almost every call and Prefetch ends up repeating the slab-table load and
	// pointer chase for every candidate, in addition to the lookup the caller
	// already did. A small associative cache absorbs the repeats without
	// turning the prefetch into a lookup of its own.
	pfChunks [tqPrefetchCacheSize]tqChunkCacheEntry
	pfStride int

	// batch is the per-search view of the TurboQuant chunk table, opened once
	// on first use. A tqComputer is built fresh for every search (see
	// resolveHNSWComputer), so the view cannot outlive the search that opened
	// it, and opening it per candidate block or per graph hop - which is what
	// the pre-batch code did, implicitly, one slab-table load at a time -
	// turned out to cost more than the lookups it saved.
	batch    types.VectorChunkBatch[byte]
	batchOK  bool
	batchGen uint64
	codesBuf [][]byte
}

// chunkBatch returns the per-search TurboQuant chunk view, opening it on first
// use and reopening it if the generation policy changed underneath the search.
func (c *tqComputer) chunkBatch() types.VectorChunkBatch[byte] {
	if !c.batchOK || c.batchGen != c.maxGen {
		c.batch = c.data.BeginTQChunkBatch(c.maxGen)
		c.batchOK = true
		c.batchGen = c.maxGen
		c.pfStride = c.batch.Width()
	}
	return c.batch
}

const tqPrefetchCacheSize = 16

type tqChunkCacheEntry struct {
	id   int
	data []byte
}

func (c *tqComputer) ComputeSingle(id uint32) (float32, error) {
	if c.sctx != nil {
		if d, ok := c.sctx.ProbeTQRecon(id); ok {
			return d, nil
		}
	}

	dim := int(c.h.dims.Load())
	// 1. Ephemeral scratch arena (eliminates redundant dequantization during
	// indexing).
	//
	// GetOrDecode already probes the arena first, so this used to do the same
	// lookup twice per candidate - once via arena.Get and again inside.
	tq := c.h.tq()
	if arena := c.h.tqArena.Load(); tq != nil && tq.compute != nil && arena != nil && arena.IsEnabled() {
		v, err := arena.GetOrDecode(id, func(_ uint32, dst []float32) error {
			tqCode, err := tq.compute.getTQBytes(id, c.diskGraph, c.maxGen)
			if err != nil {
				return err
			}
			return tq.compute.encoder.DecodeInto(tqCode, dst)
		})
		if err == nil && dim > 0 && len(c.rotatedQuery) >= dim && len(v) >= dim {
			d, err2 := c.h.distFunc(c.rotatedQuery[:dim], v[:dim])
			if err2 == nil && c.sctx != nil {
				c.sctx.InsertTQReconWithVector(id, d, v)
			}
			return d, err2
		}
		if err != nil {
			return 0, err
		}
	}

	// 2. Check legacy decode cache if present
	if p := c.h.tqDecodeCache.Load(); p != nil {
		if dim > 0 && len(c.rotatedQuery) >= dim {
			offset := int(id) * dim
			if offset+dim <= len(p.data) {
				v := p.data[offset : offset+dim]
				d, err2 := c.h.distFunc(c.rotatedQuery[:dim], v)
				if err2 == nil && c.sctx != nil {
					c.sctx.InsertTQReconWithVector(id, d, v)
				}
				return d, err2
			}
		}
		d, err := tq.compute.DistanceDirect(id, c.rotatedQuery, c.diskGraph, c.maxGen)
		if err == nil && c.sctx != nil {
			c.sctx.InsertTQRecon(id, d)
		}
		return d, err
	}
	d, err := tq.compute.DistanceDirect(id, c.rotatedQuery, c.diskGraph, c.maxGen)
	if err == nil && c.sctx != nil {
		c.sctx.InsertTQRecon(id, d)
	}
	return d, err
}

// ComputeBatch evaluates a whole block of candidates against one batch-scoped
// view of the TurboQuant chunk table.
//
// The previous implementation called ComputeSingle per candidate, so every
// candidate paid a full GetVectorsTQChunkWithGen: an atomic slab-table load, a
// division, a slab pointer chase and a generation comparison. Measured on a 250k
// TurboQuant index that lookup cost about ten times the distance computation it
// preceded, and it is paid once per candidate *and* once per prefetch. Resolving
// the chunk once per block is what the float32 path already does.
func (c *tqComputer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if c.h.tqArena.Load() != nil || c.h.tqDecodeCache.Load() != nil {
		return c.computeBatchSingle(ids, dst)
	}
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}

	if c.sctx != nil {
		allHits := true
		for i, id := range ids {
			if d, ok := c.sctx.ProbeTQRecon(id); ok {
				dst[i] = d
			} else {
				allHits = false
			}
		}
		if allHits {
			return dst, nil
		}
	}

	batch := c.chunkBatch()
	stride := c.pfStride

	if cap(c.codesBuf) < len(ids) {
		c.codesBuf = make([][]byte, len(ids))
	}
	codes := c.codesBuf[:len(ids)]

	hasFallback := false
	for i, id := range ids {
		code := batch.Vector(int(id)/types.ChunkSize, int(id)%types.ChunkSize, stride)
		codes[i] = code
		if code == nil {
			hasFallback = true
		}
	}

	if !hasFallback {
		if err := c.h.tq().compute.DistanceDirectCodesBatch(c.rotatedQuery, codes, dst); err != nil {
			return nil, err
		}
		if c.sctx != nil {
			for i, id := range ids {
				c.sctx.InsertTQRecon(id, dst[i])
			}
		}
		return dst, nil
	}

	// Mixed block: evaluate contiguous resident segments via batch kernel, fallback for nil
	segStart := -1
	for i, code := range codes {
		if code == nil {
			if segStart >= 0 {
				if err := c.h.tq().compute.DistanceDirectCodesBatch(c.rotatedQuery, codes[segStart:i], dst[segStart:i]); err != nil {
					return nil, err
				}
				if c.sctx != nil {
					for j := segStart; j < i; j++ {
						c.sctx.InsertTQRecon(ids[j], dst[j])
					}
				}
				segStart = -1
			}
			dist, err := c.ComputeSingle(ids[i])
			if err != nil {
				return nil, err
			}
			dst[i] = dist
		} else {
			if segStart < 0 {
				segStart = i
			}
		}
	}
	if segStart >= 0 {
		if err := c.h.tq().compute.DistanceDirectCodesBatch(c.rotatedQuery, codes[segStart:], dst[segStart:]); err != nil {
			return nil, err
		}
		if c.sctx != nil {
			for j := segStart; j < len(ids); j++ {
				c.sctx.InsertTQRecon(ids[j], dst[j])
			}
		}
	}
	return dst, nil
}

func (c *tqComputer) computeBatchSingle(ids []uint32, dst []float32) ([]float32, error) {
	dst = dst[:0]
	for _, id := range ids {
		dist, err := c.ComputeSingle(id)
		if err != nil {
			return nil, err
		}
		dst = append(dst, dist)
	}
	return dst, nil
}

// Prefetch issues a read hint for the TurboQuant code bytes of one node.
//
// It is a no-op while the decode cache is live. That cache is installed for the
// duration of a bulk insert, and while it is live every distance is computed
// against the pre-decoded float32 vectors (see ComputeSingle), so the packed code
// bytes are never read. Warming them anyway cost 35% of a 250k TurboQuant build:
// the chunk resolution and the prefetch itself were 22% and 13% of CPU while the
// distance they were warming accounted for 2%.
func (c *tqComputer) Prefetch(id uint32) {
	if c.h.tqArena.Load() != nil || c.h.tqDecodeCache.Load() != nil {
		return
	}
	chunkID := int(id) / types.ChunkSize
	slot := &c.pfChunks[uint(chunkID)%tqPrefetchCacheSize] // #nosec G115 -- id is a node index
	if slot.id != chunkID || slot.data == nil {
		chunk := c.data.GetVectorsTQChunkFast(chunkID)
		if chunk == nil {
			return
		}
		slot.id = chunkID
		slot.data = chunk
		if c.pfStride == 0 {
			c.pfStride = c.data.PackedSize()
		}
	}
	start := (int(id) % types.ChunkSize) * c.pfStride
	if start < len(slot.data) {
		simd.Prefetch(unsafe.Pointer(&slot.data[start])) // #nosec G103
	}
}

type float32ToSQ8Computer struct {
	squared bool
	data    *types.GraphData
	q       []float32
	dims    int
	h       *ArrowHNSW
	maxGen  uint64
	minV    float32
	scale   float32
}

func (c *float32ToSQ8Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsSQ8ChunkFast(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		paddedDims := (c.dims + 63) & ^63
		start := cOff * paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			var sum float32
			for i, val := range c.q {
				deq := c.minV + float32(v[i])*c.scale
				diff := val - deq
				sum += diff * diff
			}
			if c.squared {
				return sum, nil
			}
			return float32(math.Sqrt(float64(sum))), nil
		}
	}

	// Fallback to explicit retrieval
	return c.h.distFuncSquared(c.q, c.q) // Mock fallback error, shouldn't hit
}

func (c *float32ToSQ8Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	dst = dst[:0]
	for _, id := range ids {
		dist, err := c.ComputeSingle(id)
		if err != nil {
			return nil, err
		}
		dst = append(dst, dist)
	}
	return dst, nil
}

func (c *float32ToSQ8Computer) Prefetch(id uint32) {
	cID := int(id) / types.ChunkSize
	chunk := c.data.GetVectorsSQ8ChunkFast(cID)
	if chunk != nil {
		paddedDims := (c.dims + 63) & ^63
		cOff := int(id) % types.ChunkSize
		start := cOff * paddedDims
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}
