package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/require"
)

// TestTQScratchArenaDecodesEachVectorOnce is the decode-once guarantee the
// arena exists for. Bulk linkage revisits the same node across dozens of
// candidate hops; a regression here reverts directly to the behaviour where
// searchLayer and selectNeighbors reconstructed the same rotated vector
// millions of times per build.
func TestTQScratchArenaDecodesEachVectorOnce(t *testing.T) {
	const dims = 16

	arena := NewTQScratchArena(dims)
	defer arena.Release()

	var decodes atomic.Int64
	decode := func(id uint32, dst []float32) error {
		decodes.Add(1)
		for i := range dst {
			dst[i] = float32(id) + float32(i)
		}
		return nil
	}

	first, err := arena.GetOrDecode(7, decode)
	require.NoError(t, err)
	require.Equal(t, int64(1), decodes.Load())

	// Repeated access, including through the cheaper Get, must not decode.
	for range 100 {
		again, err := arena.GetOrDecode(7, decode)
		require.NoError(t, err)
		require.Equal(t, first, again, "the arena must return the same decoded vector")
	}

	cached, ok := arena.Get(7)
	require.True(t, ok)
	require.Equal(t, first, cached)

	require.Equal(t, int64(1), decodes.Load(), "a resident vector must be decoded exactly once")

	hits, misses, dec, chunks := arena.Stats()
	require.Equal(t, int64(1), dec)
	require.Positive(t, hits)
	require.Positive(t, chunks)

	// Every lookup before the vector was resident counts as a miss; the
	// point is that 101 further lookups added none.
	_, missesAfter, _, _ := arena.Stats()
	require.Equal(t, misses, missesAfter, "lookups of a resident vector must not count as misses")

	// And the contents must be what the decoder produced.
	require.Equal(t, float32(7), first[0])
	require.Equal(t, float32(7)+float32(dims-1), first[dims-1])
}

// TestTQScratchArenaIsBounded is the guard against the super-linear collapse.
//
// A bulk build traverses the whole resident graph, so an unbounded arena
// materialises every node it ever touches as float32 - 512 MB per 1M vectors
// at 128 dimensions. That allocator pressure is what turned a 250k TurboQuant
// build into a 1M build at ~7 vec/s. Past the cap, decodes must still happen
// and must still return the same values; they simply are not retained.
func TestTQScratchArenaIsBounded(t *testing.T) {
	const (
		dims      = 8
		maxChunks = 4
	)

	arena := NewTQScratchArena(dims)
	arena.SetMaxResidentChunks(maxChunks)
	defer arena.Release()

	decode := func(id uint32, dst []float32) error {
		for i := range dst {
			dst[i] = float32(id)
		}
		return nil
	}

	// Fill several chunks past the cap.
	total := tqArenaChunkSize * (maxChunks + 6)
	for id := range uint32(total) {
		got, err := arena.GetOrDecode(id, decode)
		require.NoErrorf(t, err, "id %d", id)
		require.Equalf(t, float32(id), got[0], "decode result for id %d", id)
		require.Len(t, got, dims)
	}

	require.LessOrEqual(t, arena.ResidentChunks(), maxChunks,
		"the arena must never retain more than its cap in decoded chunks")
	require.Positive(t, arena.Evictions(), "decodes beyond the cap must be counted")

	// Nothing that *was* retained may have been corrupted by the evictions.
	for id := range uint32(tqArenaChunkSize * 2) {
		if got, ok := arena.Get(id); ok {
			require.Equalf(t, float32(id), got[0], "retained vector %d was corrupted", id)
		}
	}
}

// TestTQScratchArenaConcurrentGetOrDecode is the thread-safety guard: bulk
// linkage shares one arena across every worker goroutine.
//
// Run with -race.
func TestTQScratchArenaConcurrentGetOrDecode(t *testing.T) {
	const (
		dims        = 16
		workers     = 8
		perWorker   = 400
		totalIDs    = tqArenaChunkSize * 3
		maxRetained = 8
	)

	arena := NewTQScratchArena(dims)
	arena.SetMaxResidentChunks(maxRetained)
	defer arena.Release()

	decode := func(id uint32, dst []float32) error {
		for i := range dst {
			dst[i] = float32(id) + float32(i)*0.5
		}
		return nil
	}

	var wg sync.WaitGroup
	var failures atomic.Int64
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range perWorker {
				id := uint32((w*perWorker + i) % totalIDs)
				got, err := arena.GetOrDecode(id, decode)
				if err != nil {
					failures.Add(1)
					return
				}
				// The value must be the one this id decodes to, whether it
				// came from the arena or from the overflow path.
				for d := range dims {
					if got[d] != float32(id)+float32(d)*0.5 {
						failures.Add(1)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	require.Zero(t, failures.Load(), "concurrent decode returned a wrong value")
	require.LessOrEqual(t, arena.ResidentChunks(), maxRetained)
}

// TestTQScratchArenaDecodeUnaffectedByEviction pins the property the overflow
// path must preserve: residency is a memory decision, not a correctness one.
func TestTQScratchArenaDecodeUnaffectedByEviction(t *testing.T) {
	const dims = 8

	decode := func(id uint32, dst []float32) error {
		for i := range dst {
			dst[i] = float32(id)*2 + float32(i)
		}
		return nil
	}

	unbounded := NewTQScratchArena(dims)
	defer unbounded.Release()
	unbounded.SetMaxResidentChunks(0)

	bounded := NewTQScratchArena(dims)
	bounded.SetMaxResidentChunks(2)
	defer bounded.Release()

	for id := range uint32(tqArenaChunkSize * 5) {
		want, err := unbounded.GetOrDecode(id, decode)
		require.NoError(t, err)
		got, err := bounded.GetOrDecode(id, decode)
		require.NoError(t, err)
		require.Equalf(t, want, got, "id %d decoded differently once the arena was capped", id)
	}

	require.Positive(t, bounded.Evictions())
}

// TestTQScratchArenaChunkTableGrowsGeometrically guards the chunk table itself.
// It was rebuilt at exactly chunkID+1 on every new maximum, which is quadratic
// in the number of chunks a large build touches.
func TestTQScratchArenaChunkTableGrowsGeometrically(t *testing.T) {
	const dims = 4

	arena := NewTQScratchArena(dims)
	defer arena.Release()

	decode := func(id uint32, dst []float32) error {
		for i := range dst {
			dst[i] = float32(id)
		}
		return nil
	}

	// Touch one new chunk per iteration; the backing array must be reused
	// rather than reallocated for every chunk index.
	arena2 := NewTQScratchArena(dims)
	defer arena2.Release()

	var tableSizes []int
	for c := range 200 {
		_, err := arena2.GetOrDecode(uint32(c*tqArenaChunkSize), decode)
		require.NoError(t, err)
		if c%50 == 0 {
			arena2.chunksMu.RLock()
			tableSizes = append(tableSizes, cap(arena2.chunks))
			arena2.chunksMu.RUnlock()
		}
	}

	require.NotEmpty(t, tableSizes)
	require.Less(t, tableSizes[len(tableSizes)-1], 1024,
		"the chunk table must not grow one entry per chunk (sizes %v)", tableSizes)
}

// TestBulkEfConstructionScalesForTurboQuant covers the construction-beam
// schedule. Every candidate in the beam costs a full polar reconstruction for
// TurboQuant, so a beam that is affordable for float32 makes the build
// super-linear; it is narrowed once the graph passes the scale at which
// full-quality search stopped being affordable, and never below the layer-0
// degree cap (a beam narrower than the links a node may hold cannot fill them,
// and an under-filled layer 0 is what makes a graph unreachable).
func TestBulkEfConstructionScalesForTurboQuant(t *testing.T) {
	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeTQ
	cfg.TurboQuantEnabled = true
	cfg.EfConstruction = 400

	tq := NewArrowHNSW(&MockDataset{Name: "tq"}, &cfg, nil)
	f32Cfg := cfg
	f32Cfg.DataType = types.VectorTypeFloat32
	f32Cfg.TurboQuantEnabled = false
	f32 := NewArrowHNSW(&MockDataset{Name: "f32"}, &f32Cfg, nil)

	floor := int(tq.mMax0.Load())
	require.Positive(t, floor)

	// Below the ramp threshold nothing narrows: a small graph is cheap, and
	// both element types use the same adaptive beam.
	require.Equal(t, f32.bulkEfConstruction(1000), tq.bulkEfConstruction(1000),
		"a young graph must use the same beam for both element types")

	// Past it, TurboQuant narrows and float32 does not. This is the whole
	// point: a beam that is affordable for a float32 kernel costs a full
	// polar reconstruction per candidate on TurboQuant.
	for _, nodeCount := range []int{turboQuantEfRampMid, turboQuantEfFullGraph, 5_000_000} {
		wantFQ := f32.bulkEfConstruction(nodeCount)
		gotTQ := tq.bulkEfConstruction(nodeCount)
		require.Lessf(t, gotTQ, wantFQ,
			"TurboQuant must use a narrower beam than float32 at %d nodes (%d vs %d)",
			nodeCount, gotTQ, wantFQ)
		require.GreaterOrEqualf(t, gotTQ, floor,
			"the TurboQuant beam must never fall below the layer-0 degree cap at %d nodes", nodeCount)
	}

	// The narrowing is monotonic in graph size.
	require.Less(t, tq.bulkEfConstruction(turboQuantEfFullGraph), tq.bulkEfConstruction(turboQuantEfRampMid),
		"the beam must keep narrowing past the mid scale")
	require.Less(t, tq.bulkEfConstruction(turboQuantEfRampMid), f32.bulkEfConstruction(turboQuantEfRampMid))
}

// TestBulkEfConstructionNeverBelowOne is a degenerate-config guard: a nonsense
// EfConstruction must not be allowed to produce an ef of zero, which would make
// the candidate beam empty and link nothing at all.
func TestBulkEfConstructionNeverBelowOne(t *testing.T) {
	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeTQ
	cfg.TurboQuantEnabled = true
	cfg.EfConstruction = 2
	cfg.M = 2
	cfg.MMax0 = 2

	h := NewArrowHNSW(&MockDataset{Name: "tq-tiny"}, &cfg, nil)
	h.efConstruction.Store(2)

	for _, nodeCount := range []int{0, 1, 1000, 2_000_000} {
		ef := h.bulkEfConstruction(nodeCount)
		require.GreaterOrEqualf(t, ef, 1, "nodeCount %d produced ef=%d", nodeCount, ef)
	}
}

// TestEnableTQScratchArenaIsIdempotent covers the arena wiring: bulk linkage
// calls EnableTQScratchArena on every batch and must get the same arena back,
// not a fresh one that discards every decoded vector so far.
func TestEnableTQScratchArenaIsIdempotent(t *testing.T) {
	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeTQ
	cfg.TurboQuantEnabled = true
	cfg.M = 16
	cfg.MMax0 = 16

	h := NewArrowHNSW(&MockDataset{Name: "tq-arena"}, &cfg, nil)

	first := h.EnableTQScratchArena()
	require.NotNil(t, first)
	require.True(t, first.IsEnabled())

	decode := func(id uint32, dst []float32) error {
		for i := range dst {
			dst[i] = float32(id)
		}
		return nil
	}
	_, err := first.GetOrDecode(3, decode)
	require.NoError(t, err)

	second := h.EnableTQScratchArena()
	require.Same(t, first, second, "a second bulk batch must reuse the warm arena")
	require.Same(t, second, h.GetTQScratchArena())

	_, ok := second.Get(3)
	require.True(t, ok, "re-enabling must not discard already decoded vectors")

	h.ReleaseTQScratchArena()
	require.Nil(t, h.GetTQScratchArena())
}

// TestTQScratchArenaZeroDimsDoesNotPanic covers the degenerate dimension: a
// zero-width arena must still answer GetOrDecode without indexing a chunk out
// of range.
func TestTQScratchArenaZeroDimsDoesNotPanic(t *testing.T) {
	arena := NewTQScratchArena(0)
	defer arena.Release()

	require.NotPanics(t, func() {
		_, err := arena.GetOrDecode(0, func(_ uint32, dst []float32) error {
			return fmt.Errorf("decode of a zero-width vector must not be reachable")
		})
		require.Error(t, err)
	})
}
