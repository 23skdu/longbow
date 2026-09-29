package memory

import (
	"math"
	"testing"
)

// benchArenaChunks returns an arena pre-filled with n chunks of chunkLen bytes
// at 128-byte stride, mirroring how GraphData lays out vector chunks.
func benchArenaChunks(n, chunkLen int, elemLen int) (*SlabArena, []uint64) {
	arena := NewSlabArena(1 << 20)
	offsets := make([]uint64, n)
	for i := 0; i < n; i++ {
		off, err := arena.AllocDirty(chunkLen * elemLen)
		if err != nil {
			panic(err)
		}
		offsets[i] = off
		data := arena.Get(off, uint32(chunkLen*elemLen)) // #nosec G115
		for j := range data {
			data[j] = byte(i + j)
		}
	}
	return arena, offsets
}

func BenchmarkSlabArena_GetWithGeneration(b *testing.B) {
	const chunkLen = 1024
	arena, offsets := benchArenaChunks(4, chunkLen, 4)
	gen := arena.GetGeneration()
	b.ReportAllocs()
	b.ResetTimer()

	var sink byte
	for b.Loop() {
		for _, off := range offsets {
			v := arena.GetWithGeneration(off, uint32(chunkLen*4), gen) // #nosec G115
			sink += v[0]
		}
	}
	_ = sink
}

func BenchmarkSlabArena_GetWithGeneration_MaxUint64(b *testing.B) {
	const chunkLen = 1024
	arena, offsets := benchArenaChunks(4, chunkLen, 4)
	b.ReportAllocs()
	b.ResetTimer()

	var sink byte
	for b.Loop() {
		for _, off := range offsets {
			v := arena.GetWithGeneration(off, uint32(chunkLen*4), math.MaxUint64) // #nosec G115
			sink += v[0]
		}
	}
	_ = sink
}

// benchBatchIDs returns n consecutive vector offsets inside one chunk.
func benchBatchIDs(arena *SlabArena, n, vecBytes int) []uint64 {
	off, err := arena.AllocDirty(n * vecBytes)
	if err != nil {
		panic(err)
	}
	data := arena.Get(off, uint32(n*vecBytes)) // #nosec G115
	for i := range data {
		data[i] = byte(i)
	}
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = off + uint64(i*vecBytes) // #nosec G115
	}
	return ids
}

// BenchmarkSlabArena_BatchResolve_* measures the amortised per-vector cost of
// resolving a whole batch through one SlabBatch versus the per-lookup
// GetWithGeneration cost.
func benchmarkSlabBatchResolve(b *testing.B, n int) {
	const vecBytes = 512 // 128 float32
	arena, _ := benchArenaChunks(1, n*vecBytes, 1)
	gen := arena.GetGeneration()
	ids := benchBatchIDs(arena, n, vecBytes)

	b.ReportAllocs()
	b.ResetTimer()

	var sink byte
	for b.Loop() {
		batch := arena.BeginBatch(gen)
		for _, id := range ids {
			v := batch.Get(id, vecBytes)
			sink += v[0]
		}
	}
	_ = sink
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(int64(b.N)*int64(n)), "ns/vector")
}

func BenchmarkSlabArena_BatchResolve_32(b *testing.B)  { benchmarkSlabBatchResolve(b, 32) }
func BenchmarkSlabArena_BatchResolve_256(b *testing.B) { benchmarkSlabBatchResolve(b, 256) }
func BenchmarkSlabArena_BatchResolve_1024(b *testing.B) {
	benchmarkSlabBatchResolve(b, 1024)
}

// BenchmarkSlabArena_GetWithGeneration_PerVector_* is the matching baseline:
// the same id walk, resolved one lookup at a time.
func benchmarkSlabGetPerVector(b *testing.B, n int) {
	const vecBytes = 512
	arena, _ := benchArenaChunks(1, n*vecBytes, 1)
	gen := arena.GetGeneration()
	ids := benchBatchIDs(arena, n, vecBytes)

	b.ReportAllocs()
	b.ResetTimer()

	var sink byte
	for b.Loop() {
		for _, id := range ids {
			v := arena.GetWithGeneration(id, vecBytes, gen)
			sink += v[0]
		}
	}
	_ = sink
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(int64(b.N)*int64(n)), "ns/vector")
}

func BenchmarkSlabArena_GetWithGeneration_PerVector_32(b *testing.B) {
	benchmarkSlabGetPerVector(b, 32)
}

func BenchmarkSlabArena_GetWithGeneration_PerVector_256(b *testing.B) {
	benchmarkSlabGetPerVector(b, 256)
}

func BenchmarkSlabArena_GetWithGeneration_PerVector_1024(b *testing.B) {
	benchmarkSlabGetPerVector(b, 1024)
}

// BenchmarkTypedArena_GetWithGeneration vs BenchmarkTypedArena_BatchResolve
// covers the typed layer the graph-data wrappers sit on.
func BenchmarkTypedArena_GetWithGeneration(b *testing.B) {
	const vecElems = 128
	arena := NewSlabArena(1 << 20)
	ta := NewTypedArena[float32](arena)
	ref, err := ta.AllocSlice(vecElems * 1024)
	if err != nil {
		b.Fatal(err)
	}
	v := ta.Get(ref)
	for i := range v {
		v[i] = float32(i)
	}
	refs := make([]SliceRef, 1024)
	for i := range refs {
		refs[i] = SliceRef{Offset: ref.Offset + uint64(i*vecElems*4), Len: vecElems, Cap: vecElems} // #nosec G115
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		for _, r := range refs {
			sink += ta.GetWithGeneration(r, math.MaxUint64)[0]
		}
	}
	_ = sink
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*1024), "ns/vector")
}

func BenchmarkTypedArena_BatchResolve(b *testing.B) {
	const vecElems = 128
	arena := NewSlabArena(1 << 20)
	ta := NewTypedArena[float32](arena)
	ref, err := ta.AllocSlice(vecElems * 1024)
	if err != nil {
		b.Fatal(err)
	}
	v := ta.Get(ref)
	for i := range v {
		v[i] = float32(i)
	}
	refs := make([]SliceRef, 1024)
	for i := range refs {
		refs[i] = SliceRef{Offset: ref.Offset + uint64(i*vecElems*4), Len: vecElems, Cap: vecElems} // #nosec G115
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for b.Loop() {
		batch := ta.BeginBatch(math.MaxUint64)
		for _, r := range refs {
			sink += batch.Get(r)[0]
		}
	}
	_ = sink
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*1024), "ns/vector")
}
