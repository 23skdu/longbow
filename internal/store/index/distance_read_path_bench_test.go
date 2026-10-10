package index

import (
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// The 16-bit read path against the 8-bit read path, at the scale where the
// difference is memory rather than arithmetic.
//
// The 100k benchmark matrix reported int16 and uint16 dense search at roughly a
// sixth of the 8-bit types (docs/roadmap.md item 1, R32). That comparison was
// not measuring the read path: the query reaching these types was converted by
// a truncating cast, so a [0,1) probe against a [0,127) corpus collapsed to the
// zero vector and every query degenerated to "nearest node to the origin". The
// QPS that came out was a property of tie-breaking order, not of element width.
//
// This benchmark is the measurement that does isolate the read path: the same
// random id stream, over the same graph topology, at a working set that exceeds
// the last-level cache, for every element width. The 16-bit types move twice the
// bytes of the 8-bit ones, so this is where a real width penalty would show up
// and where it is bounded by memory bandwidth rather than by the kernel.

const (
	readPathScale = 500_000
	readPathDims  = 128
	readPathBlock = 64 // searchLayer's traversalBlockSize
)

// buildReadPathGraph populates a GraphData of dt with n random vectors.
func buildReadPathGraph(tb testing.TB, dt types.VectorDataType, n, dims int) *types.GraphData {
	tb.Helper()
	g := types.NewGraphData(n, dims, false, false, -1, false, false, false,
		dt, false, false, false, 8, "read-path", nil, false)
	if g == nil {
		tb.Fatal("NewGraphData returned nil")
	}
	for c := 0; c < n/types.ChunkSize; c++ {
		if err := g.EnsureChunk(c, 0, dims); err != nil {
			tb.Fatalf("EnsureChunk(%d): %v", c, err)
		}
	}
	rng := rand.New(rand.NewSource(11)) // #nosec G404 -- benchmark data
	buf := make([]float32, dims)
	for id := 0; id < n; id++ {
		for j := range buf {
			buf[j] = rng.Float32()*2 - 1
		}
		var vec any
		switch dt {
		case types.VectorTypeInt8:
			v := make([]int8, dims)
			for j := range buf {
				v[j] = int8(buf[j] * 100)
			}
			vec = v
		case types.VectorTypeUint8:
			v := make([]uint8, dims)
			for j := range buf {
				v[j] = uint8((buf[j] + 1) * 100)
			}
			vec = v
		case types.VectorTypeInt16:
			v := make([]int16, dims)
			for j := range buf {
				v[j] = int16(buf[j] * 30000)
			}
			vec = v
		case types.VectorTypeUint16:
			v := make([]uint16, dims)
			for j := range buf {
				v[j] = uint16((buf[j] + 1) * 30000)
			}
			vec = v
		}
		if err := g.SetVector(uint32(id), vec); err != nil { // #nosec G115
			tb.Fatalf("SetVector(%d): %v", id, err)
		}
	}
	return g
}

// readPathIDs returns random ids in blocks of traversalBlockSize, which is how
// searchLayer walks a neighbour list: the ids in one hop are neighbours of one
// node and are close together in the graph but scattered across the arena.
func readPathIDs(n, blocks int) [][]uint32 {
	rng := rand.New(rand.NewSource(23)) // #nosec G404 -- benchmark data
	out := make([][]uint32, blocks)
	for i := range out {
		blk := make([]uint32, readPathBlock)
		for j := range blk {
			blk[j] = uint32(rng.Intn(n)) // #nosec G115
		}
		out[i] = blk
	}
	return out
}

func newReadPathHNSW() *ArrowHNSW {
	return &ArrowHNSW{
		distFunc:            simd.EuclideanDistance,
		distFuncSquared:     simd.L2Squared,
		distFuncF64:         simd.EuclideanDistanceFloat64,
		distFuncInt8:        simd.EuclideanDistanceInt8,
		distFuncInt8Squared: simd.L2SquaredInt8,
		distFuncUint8:       simd.EuclideanDistanceUint8,
		distFuncInt16:       simd.EuclideanDistanceInt16,
		distFuncUint16:      simd.EuclideanDistanceUint16,
		config:              types.ArrowHNSWConfig{},
	}
}

// BenchmarkComputeBatch_ReadPath measures chunk resolution, memory fetch and
// the distance kernel together, over one shared random id stream.
//
// One op is len(blocks) * readPathBlock = 32768 distance evaluations, so
// ns/op / 32768 is the per-vector cost.
//
// Reference figures on the i7-12650H used for docs/performance.md (AVX2, no
// AVX-512, 24 MiB L3, 500k x 128-d, so 64 MiB for a 1-byte type and 128 MiB for
// a 2-byte type - neither resident), 32768 evaluations per op:
//
//	int8    1875378 ns/op -> 57.2 ns/vector
//	int16   2659604 ns/op -> 81.2 ns/vector  (1.42x)
//	uint16  2738711 ns/op -> 83.6 ns/vector  (1.46x)
//
// All three are zero-allocation. The 1.4x ratio tracks the 2x byte count, so
// the read path costs roughly what its memory traffic says it costs. Before the
// int16/uint16 kernels were rebuilt the ratio was 1.69x. Neither figure supports
// the 6x claim in docs/roadmap.md item 1.
func BenchmarkComputeBatch_ReadPath(b *testing.B) {
	blocks := readPathIDs(readPathScale, 512)
	dst := make([]float32, readPathBlock)

	run := func(b *testing.B, compute func([]uint32, []float32) ([]float32, error)) {
		b.Helper()
		var sink float32
		for b.Loop() {
			for _, blk := range blocks {
				out, err := compute(blk, dst)
				if err != nil {
					b.Fatal(err)
				}
				sink += out[0]
			}
		}
		_ = sink
	}

	b.Run("Int8", func(b *testing.B) {
		g := buildReadPathGraph(b, types.VectorTypeInt8, readPathScale, readPathDims)
		q := make([]uint8, readPathDims)
		for i := range q {
			q[i] = uint8(i % 200)
		}
		qI8 := make([]int8, len(q))
		for i, v := range q {
			qI8[i] = int8(v)
		}
		c := &int8Computer{data: g, q: q, qInt8: qI8, dims: readPathDims, h: newReadPathHNSW(), maxGen: math.MaxUint64}
		b.ReportAllocs()
		run(b, c.ComputeBatch)
	})

	b.Run("Int16", func(b *testing.B) {
		g := buildReadPathGraph(b, types.VectorTypeInt16, readPathScale, readPathDims)
		q := make([]int16, readPathDims)
		for i := range q {
			q[i] = int16((i * 7) % 1000)
		}
		c := &int16Computer{data: g, q: q, dims: readPathDims, h: newReadPathHNSW(), maxGen: math.MaxUint64}
		b.ReportAllocs()
		run(b, c.ComputeBatch)
	})

	b.Run("Uint16", func(b *testing.B) {
		g := buildReadPathGraph(b, types.VectorTypeUint16, readPathScale, readPathDims)
		q := make([]uint16, readPathDims)
		for i := range q {
			q[i] = uint16((i * 11) % 1000)
		}
		c := &uint16Computer{data: g, q: q, dims: readPathDims, h: newReadPathHNSW(), maxGen: math.MaxUint64}
		b.ReportAllocs()
		run(b, c.ComputeBatch)
	})
}
