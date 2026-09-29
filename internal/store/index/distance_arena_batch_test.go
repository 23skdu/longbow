package index

import (
	"math"
	"testing"
	"unsafe"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/require"
)

// batchParityGraphData builds a GraphData with all chunks resident plus one id
// range that is deliberately not resident, so the batch path and the reference
// path both have to fall back.
func batchParityGraphData(t *testing.T, dt types.VectorDataType, dims, nChunks int) *types.GraphData {
	t.Helper()
	g := types.NewGraphData(nChunks*types.ChunkSize, dims, false, false, -1, false, false, false,
		dt, false, false, false, 8, "batch-parity", nil, false)
	require.NotNil(t, g)
	for c := 0; c < nChunks; c++ {
		require.NoError(t, g.EnsureChunk(c, 0, dims))
	}
	for id := 0; id < nChunks*types.ChunkSize; id++ {
		var vec any
		switch dt {
		case types.VectorTypeFloat32:
			v := make([]float32, dims)
			for i := range v {
				v[i] = float32(id*dims+i) / 7.0
			}
			vec = v
		case types.VectorTypeFloat64:
			v := make([]float64, dims)
			for i := range v {
				v[i] = float64(id*dims+i) / 11.0
			}
			vec = v
		case types.VectorTypeInt8:
			v := make([]int8, dims)
			for i := range v {
				v[i] = int8((id*dims + i) % 251)
			}
			vec = v
		}
		require.NoError(t, g.SetVector(uint32(id), vec)) // #nosec G115
	}
	return g
}

// batchParityIDs walks chunk boundaries, the last id of a chunk, an id past the
// resident range and an id that wraps a chunk with no data.
func batchParityIDs(nChunks int) []uint32 {
	ids := []uint32{0, 1, 1023}
	for c := 0; c < nChunks; c++ {
		base := uint32(c * types.ChunkSize)
		ids = append(ids, base, base+1, base+511, base+1023)
	}
	resident := nChunks * types.ChunkSize
	ids = append(ids, uint32(resident), uint32(resident)+5, ^uint32(0)-1)
	return ids
}

func TestComputeBatch_Float32MatchesComputeSingle(t *testing.T) {
	const nChunks = 3
	for _, maxGen := range []uint64{math.MaxUint64, 0, 1} {
		g := batchParityGraphData(t, types.VectorTypeFloat32, 64, nChunks)
		q := make([]float32, 64)
		for i := range q {
			q[i] = float32(i) / 3.0
		}
		for _, squared := range []bool{false, true} {
			c := &float32Computer{
				data:    g,
				q:       q,
				dims:    64,
				h:       newBatchBenchHNSW(),
				maxGen:  maxGen,
				squared: squared,
			}
			ids := batchParityIDs(nChunks)
			want := make([]float32, 0, len(ids))
			for _, id := range ids {
				d, err := c.ComputeSingle(id)
				require.NoError(t, err)
				want = append(want, d)
			}

			got, err := c.ComputeBatch(ids, make([]float32, 0, len(ids)))
			require.NoError(t, err)
			require.Len(t, got, len(ids), "maxGen=%d squared=%v", maxGen, squared)
			for i := range want {
				require.Equal(t, want[i], got[i],
					"bit-identical at i=%d id=%d maxGen=%d squared=%v", i, ids[i], maxGen, squared)
			}
		}
	}
}

func TestComputeBatch_Float64MatchesComputeSingle(t *testing.T) {
	const nChunks = 2
	for _, maxGen := range []uint64{math.MaxUint64, 0} {
		g := batchParityGraphData(t, types.VectorTypeFloat64, 64, nChunks)
		q := make([]float64, 64)
		for i := range q {
			q[i] = float64(i) / 5.0
		}
		c := &float64Computer{
			data:   g,
			q:      q,
			dims:   64,
			h:      newBatchBenchHNSW(),
			maxGen: maxGen,
		}
		ids := batchParityIDs(nChunks)
		want := make([]float32, 0, len(ids))
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			require.NoError(t, err)
			want = append(want, d)
		}

		got, err := c.ComputeBatch(ids, make([]float32, 0, len(ids)))
		require.NoError(t, err)
		require.Len(t, got, len(ids))
		for i := range want {
			require.Equal(t, want[i], got[i], "bit-identical at i=%d id=%d maxGen=%d", i, ids[i], maxGen)
		}
	}
}

func TestComputeBatch_Int8MatchesComputeSingle(t *testing.T) {
	const nChunks = 2
	for _, maxGen := range []uint64{math.MaxUint64, 0} {
		for _, squared := range []bool{false, true} {
			g := batchParityGraphData(t, types.VectorTypeInt8, 64, nChunks)
			q := make([]uint8, 64)
			for i := range q {
				q[i] = byte(i)
			}
			qI8 := unsafe.Slice((*int8)(unsafe.Pointer(&q[0])), len(q)) // #nosec G103
			c := &int8Computer{
				data:    g,
				q:       q,
				qInt8:   qI8,
				dims:    64,
				h:       newBatchBenchHNSW(),
				maxGen:  maxGen,
				squared: squared,
			}
			ids := batchParityIDs(nChunks)
			want := make([]float32, 0, len(ids))
			for _, id := range ids {
				d, err := c.ComputeSingle(id)
				require.NoError(t, err)
				want = append(want, d)
			}

			got, err := c.ComputeBatch(ids, make([]float32, 0, len(ids)))
			require.NoError(t, err)
			require.Len(t, got, len(ids))
			for i := range want {
				require.Equal(t, want[i], got[i],
					"bit-identical at i=%d id=%d maxGen=%d squared=%v", i, ids[i], maxGen, squared)
			}
		}
	}
}

func TestComputeBatch_Float32ToFloat32MatchesComputeSingle(t *testing.T) {
	const nChunks = 2
	g := batchParityGraphData(t, types.VectorTypeFloat32, 64, nChunks)
	q := make([]float32, 64)
	for i := range q {
		q[i] = float32(i) / 2.0
	}
	h := newBatchBenchHNSW()
	// Only resident ids: for a paged-out id ComputeBatch has always differed
	// from ComputeSingle (it leaves a nil vector in the batch and the SIMD
	// batch kernel yields 0 where ComputeSingle yields MaxFloat32). That
	// divergence predates the arena batch and is out of scope here.
	ids := []uint32{0, 1, 511, 1023, 1024, 1025, 2046, 2047}
	for _, maxGen := range []uint64{math.MaxUint64, 0} {
		for _, squared := range []bool{false, true} {
			c := &float32ToFloat32Computer{
				data:    g,
				q:       q,
				dims:    64,
				h:       h,
				maxGen:  maxGen,
				squared: squared,
			}
			want := make([]float32, 0, len(ids))
			for _, id := range ids {
				d, err := c.ComputeSingle(id)
				require.NoError(t, err)
				want = append(want, d)
			}

			got, err := c.ComputeBatch(ids, make([]float32, 0, len(ids)))
			require.NoError(t, err)
			require.Len(t, got, len(ids))
			for i := range want {
				require.Equal(t, want[i], got[i],
					"bit-identical at i=%d id=%d maxGen=%d squared=%v", i, ids[i], maxGen, squared)
			}
		}
	}
}

func TestComputeBatch_EmptyAndNilDst(t *testing.T) {
	g := batchParityGraphData(t, types.VectorTypeFloat32, 32, 1)
	h := newBatchBenchHNSW()
	q := make([]float32, 32)

	f32 := &float32Computer{data: g, q: q, dims: 32, h: h, maxGen: math.MaxUint64}
	got, err := f32.ComputeBatch(nil, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	q64 := make([]float64, 32)
	f64 := &float64Computer{data: batchParityGraphData(t, types.VectorTypeFloat64, 32, 1),
		q: q64, dims: 32, h: h, maxGen: math.MaxUint64}
	got, err = f64.ComputeBatch(nil, nil)
	require.NoError(t, err)
	require.Empty(t, got)

	qu8, qu8i8 := batchBenchInt8Query()
	i8 := &int8Computer{data: batchParityGraphData(t, types.VectorTypeInt8, 32, 1),
		q: qu8, qInt8: qu8i8, dims: 32, h: h, maxGen: math.MaxUint64}
	got, err = i8.ComputeBatch(nil, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}
