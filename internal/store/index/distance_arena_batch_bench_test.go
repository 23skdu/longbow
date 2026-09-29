package index

import (
	"math"
	"testing"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

const (
	batchBenchDims   = 128
	batchBenchVector = 4096
)

// newBatchBenchGraphData builds a GraphData with every chunk resident in its
// typed arena so the distance computers exercise the arena read path.
func newBatchBenchGraphData(tb testing.TB, dt types.VectorDataType) *types.GraphData {
	tb.Helper()

	g := types.NewGraphData(batchBenchVector, batchBenchDims, false, false, -1, false, false, false,
		dt, false, false, false, 8, "batch-bench", nil, false)
	if g == nil {
		tb.Fatal("NewGraphData returned nil")
	}

	numChunks := batchBenchVector / types.ChunkSize
	for c := 0; c < numChunks; c++ {
		if err := g.EnsureChunk(c, 0, batchBenchDims); err != nil {
			tb.Fatalf("EnsureChunk(%d): %v", c, err)
		}
	}

	rng := uint64(0x9E3779B97F4A7C15)
	next := func() float32 {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return float32(int32(rng%2001)-1000) / 1000.0
	}

	for id := 0; id < batchBenchVector; id++ {
		var vec any
		switch dt {
		case types.VectorTypeFloat32:
			v := make([]float32, batchBenchDims)
			for i := range v {
				v[i] = next()
			}
			vec = v
		case types.VectorTypeFloat64:
			v := make([]float64, batchBenchDims)
			for i := range v {
				v[i] = float64(next())
			}
			vec = v
		case types.VectorTypeInt8:
			v := make([]int8, batchBenchDims)
			for i := range v {
				v[i] = int8(next() * 100)
			}
			vec = v
		}
		if err := g.SetVector(uint32(id), vec); err != nil { // #nosec G115
			tb.Fatalf("SetVector(%d): %v", id, err)
		}
	}
	return g
}

// batchBenchIDs returns a contiguous id walk that crosses chunk boundaries,
// mirroring the neighbour iteration order of an HNSW search.
func batchBenchIDs() []uint32 {
	ids := make([]uint32, batchBenchVector)
	for i := range ids {
		ids[i] = uint32(i) // #nosec G115
	}
	return ids
}

func newBatchBenchHNSW() *ArrowHNSW {
	return &ArrowHNSW{
		distFunc:            simd.EuclideanDistance,
		distFuncSquared:     simd.L2Squared,
		distFuncF64:         simd.EuclideanDistanceFloat64,
		distFuncInt8:        simd.EuclideanDistanceInt8,
		distFuncInt8Squared: simd.L2SquaredInt8,
		config:              types.ArrowHNSWConfig{},
	}
}

func batchBenchInt8Query() ([]uint8, []int8) {
	q := make([]uint8, batchBenchDims)
	for i := range q {
		q[i] = byte(i)
	}
	return q, unsafe.Slice((*int8)(unsafe.Pointer(&q[0])), len(q)) // #nosec G103
}

// The Legacy* benchmarks below replicate the pre-change ComputeBatch bodies
// verbatim (one arena lookup per vector, or per chunk for the int8 loop) so
// before/after can be compared in a single run on the same machine state.

func benchmarkComputeBatchFloat32Legacy(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeFloat32)
	c := &float32Computer{
		data:   g,
		q:      g.GetVectorsChunkFast(0)[:batchBenchDims],
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()
	dst := make([]float32, 0, len(ids))

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				b.Fatal(err)
			}
			dst = append(dst, d)
		}
		sink += dst[0]
	}
	_ = sink
}

func benchmarkComputeBatchFloat64Legacy(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeFloat64)
	c := &float64Computer{
		data:   g,
		q:      g.GetVectorsFloat64ChunkFast(0)[:batchBenchDims],
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()
	dst := make([]float32, len(ids))
	n := len(ids)

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		const blockSize = 64
		for blockStart := 0; blockStart < n; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > n {
				blockEnd = n
			}
			if blockEnd < n {
				nextEnd := blockEnd + blockSize
				if nextEnd > n {
					nextEnd = n
				}
				for _, nextID := range ids[blockEnd:nextEnd] {
					c.Prefetch(nextID)
				}
			}
			for i := blockStart; i < blockEnd; i++ {
				id := ids[i]
				cID := types.ChunkID(id)
				var chunk []float64
				if c.maxGen == math.MaxUint64 {
					chunk = g.GetVectorsFloat64ChunkFast(int(cID))
				} else {
					chunk = g.GetVectorsFloat64ChunkWithGen(int(cID), c.maxGen)
				}
				if chunk != nil {
					cOff := int(id) % types.ChunkSize
					pd := g.GetPaddedDimsForType(types.VectorTypeFloat64)
					start := cOff * pd
					if start+c.dims <= len(chunk) {
						d, err := c.h.distFuncF64(c.q, chunk[start:start+c.dims])
						if err != nil {
							dst[i] = math.MaxFloat32
							continue
						}
						dst[i] = d
						continue
					}
				}
				dst[i] = math.MaxFloat32
			}
		}
		sink += dst[0]
	}
	_ = sink
}

func benchmarkComputeBatchInt8Legacy(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeInt8)
	q, qI8 := batchBenchInt8Query()
	c := &int8Computer{
		data:   g,
		q:      q,
		qInt8:  qI8,
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()
	dst := make([]float32, len(ids))
	pd := g.GetPaddedDimsForType(types.VectorTypeInt8)

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		var lastChunkID int32 = -1
		var chunk []int8
		for i, id := range ids {
			cID := int32(types.ChunkID(id)) // #nosec G115
			if cID != lastChunkID {
				lastChunkID = cID
				chunk = g.GetVectorsInt8ChunkFast(int(cID))
			}
			if chunk != nil {
				start := (int(id) % types.ChunkSize) * pd
				if start+c.dims <= len(chunk) {
					d, _ := c.h.distFuncInt8(c.qInt8, chunk[start:start+c.dims])
					dst[i] = d
					continue
				}
			}
			d, err := c.ComputeSingle(id)
			if err != nil {
				b.Fatal(err)
			}
			dst[i] = d
		}
		sink += dst[0]
	}
	_ = sink
}

func BenchmarkComputeBatch_Float32_LegacyLoop(b *testing.B) { benchmarkComputeBatchFloat32Legacy(b) }
func BenchmarkComputeBatch_Float64_LegacyLoop(b *testing.B) { benchmarkComputeBatchFloat64Legacy(b) }
func BenchmarkComputeBatch_Int8_LegacyLoop(b *testing.B)    { benchmarkComputeBatchInt8Legacy(b) }

func BenchmarkComputeBatch_Float32_SingleLookup(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeFloat32)
	c := &float32Computer{
		data:   g,
		q:      g.GetVectorsChunkFast(0)[:batchBenchDims],
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				b.Fatal(err)
			}
			sink += d
		}
	}
	_ = sink
}

func BenchmarkComputeBatch_Float32_BatchSnapshot(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeFloat32)
	c := &float32Computer{
		data:   g,
		q:      g.GetVectorsChunkFast(0)[:batchBenchDims],
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()
	dst := make([]float32, len(ids))

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		out, err := c.ComputeBatch(ids, dst)
		if err != nil {
			b.Fatal(err)
		}
		sink += out[0]
	}
	_ = sink
}

func BenchmarkComputeBatch_Float64_SingleLookup(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeFloat64)
	c := &float64Computer{
		data:   g,
		q:      g.GetVectorsFloat64ChunkFast(0)[:batchBenchDims],
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				b.Fatal(err)
			}
			sink += d
		}
	}
	_ = sink
}

func BenchmarkComputeBatch_Float64_BatchSnapshot(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeFloat64)
	c := &float64Computer{
		data:   g,
		q:      g.GetVectorsFloat64ChunkFast(0)[:batchBenchDims],
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()
	dst := make([]float32, len(ids))

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		out, err := c.ComputeBatch(ids, dst)
		if err != nil {
			b.Fatal(err)
		}
		sink += out[0]
	}
	_ = sink
}

func BenchmarkComputeBatch_Int8_SingleLookup(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeInt8)
	q, qI8 := batchBenchInt8Query()
	c := &int8Computer{
		data:   g,
		q:      q,
		qInt8:  qI8,
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				b.Fatal(err)
			}
			sink += d
		}
	}
	_ = sink
}

func BenchmarkComputeBatch_Int8_BatchSnapshot(b *testing.B) {
	g := newBatchBenchGraphData(b, types.VectorTypeInt8)
	q, qI8 := batchBenchInt8Query()
	c := &int8Computer{
		data:   g,
		q:      q,
		qInt8:  qI8,
		dims:   batchBenchDims,
		h:      newBatchBenchHNSW(),
		maxGen: math.MaxUint64,
	}
	ids := batchBenchIDs()
	dst := make([]float32, len(ids))

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		out, err := c.ComputeBatch(ids, dst)
		if err != nil {
			b.Fatal(err)
		}
		sink += out[0]
	}
	_ = sink
}
