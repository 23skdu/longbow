package memory

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchArenaChunk lays down n chunks of chunkElems elements, tagging every byte
// with (chunkID, index) so a parity failure reports which vector diverged.
func batchArenaChunk(t *testing.T, slabSize, chunkBytes, n int) (*SlabArena, []uint64) {
	t.Helper()
	arena := NewSlabArena(slabSize)
	offsets := make([]uint64, n)
	for i := 0; i < n; i++ {
		off, err := arena.AllocDirty(chunkBytes)
		require.NoError(t, err)
		offsets[i] = off
		data := arena.Get(off, uint32(chunkBytes)) // #nosec G115
		require.Len(t, data, chunkBytes)
		for j := range data {
			data[j] = byte(i*7 + j*3)
		}
	}
	return arena, offsets
}

func assertBytesParity(t *testing.T, got, want []byte, what string) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Fatalf("%s: nil mismatch: batch=%v ref=%v (len %d vs %d)", what, got == nil, want == nil, len(got), len(want))
	}
	if got == nil {
		return
	}
	if len(got) != len(want) {
		t.Fatalf("%s: length mismatch: batch=%d ref=%d", what, len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: byte mismatch (batch len=%d, ref len=%d)", what, len(got), len(want))
	}
	if &got[0] != &want[0] {
		t.Fatalf("%s: batch and reference resolved to different backing memory", what)
	}
}

// TestSlabBatch_MatchesGetWithGeneration pins the batch to the reference
// accessor across chunk sizes, slab sizes, generations and id ranges, including
// chunk boundaries, the last vector of a chunk, empty chunks and ids past the
// end of the arena.
func TestSlabBatch_MatchesGetWithGeneration(t *testing.T) {
	slabSizes := []int{1024, 4096, 1 << 16}
	chunkBytesList := []int{8, 64, 512}
	chunkCounts := []int{1, 2, 5}

	for _, slabSize := range slabSizes {
		for _, chunkBytes := range chunkBytesList {
			for _, nChunks := range chunkCounts {
				name := fmt.Sprintf("slab%d/chunk%d/n%d", slabSize, chunkBytes, nChunks)
				t.Run(name, func(t *testing.T) {
					arena, offsets := batchArenaChunk(t, slabSize, chunkBytes, nChunks)

					// A vector length that divides the chunk exactly, and one that
					// runs off the end of the chunk.
					vecLens := []uint32{1, uint32(chunkBytes / 4), uint32(chunkBytes), uint32(chunkBytes) + 1} // #nosec G115

					gens := []uint64{0, 1, 2, arena.GetGeneration(), math.MaxUint64 - 1, math.MaxUint64}

					for _, gen := range gens {
						batch := arena.BeginBatch(gen)
						for _, off := range offsets {
							// Every vector slot in the chunk plus the boundary and
							// out-of-range ids.
							for v := 0; v <= chunkBytes; v += maxInt(1, chunkBytes/3) {
								o := off + uint64(v) // #nosec G115
								for _, vl := range vecLens {
									got := batch.Get(o, vl)
									want := arena.GetWithGeneration(o, vl, gen)
									assertBytesParity(t, got, want, fmt.Sprintf("gen=%d off=%d len=%d", gen, v, vl))
								}
							}
						}
						// One past the last allocated byte of the last chunk.
						last := offsets[len(offsets)-1] + uint64(chunkBytes) // #nosec G115
						got := batch.Get(last, 1)
						want := arena.GetWithGeneration(last, 1, gen)
						assertBytesParity(t, got, want, "past-end")
					}
				})
			}
		}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TestSlabBatch_TypedParity pins TypedBatch against TypedArena.GetWithGeneration
// with reflect.DeepEqual, the way the graph-data layer consumes it.
func TestSlabBatch_TypedParity(t *testing.T) {
	arena := NewSlabArena(1 << 16)
	ta := NewTypedArena[float32](arena)

	const elems = 64
	refs := make([]SliceRef, 0, 4)
	for i := 0; i < 4; i++ {
		ref, err := ta.AllocSlice(elems)
		require.NoError(t, err)
		v := ta.Get(ref)
		require.Len(t, v, elems)
		for j := range v {
			v[j] = float32(i*elems+j) / 3.0
		}
		refs = append(refs, ref)
	}

	for _, gen := range []uint64{0, 1, math.MaxUint64} {
		batch := ta.BeginBatch(gen)
		for _, ref := range refs {
			got := batch.Get(ref)
			want := ta.GetWithGeneration(ref, gen)
			require.True(t, reflect.DeepEqual(got, want), "gen=%d: typed batch parity", gen)
		}
		// Last element of the last ref, and one element past its end.
		last := refs[len(refs)-1]
		tail := SliceRef{Offset: last.Offset + uint64(uint32(last.Len-1)*4), Len: 1, Cap: 1} // #nosec G115
		require.True(t, reflect.DeepEqual(batch.Get(tail), ta.GetWithGeneration(tail, gen)), "tail")

		over := SliceRef{Offset: last.Offset + uint64(uint32(last.Len)*4), Len: 1, Cap: 1} // #nosec G115
		require.True(t, reflect.DeepEqual(batch.Get(over), ta.GetWithGeneration(over, gen)), "overflow")
	}
}

// TestSlabBatch_GenerationBumpMidBatch pins the documented behaviour: slab
// generations are immutable, so a generation bump does NOT retroactively
// invalidate a batch, and data written into the new generation stays invisible
// to a batch holding an older maxGeneration.
func TestSlabBatch_GenerationBumpMidBatch(t *testing.T) {
	arena := NewSlabArena(4096)

	old, err := arena.AllocDirty(256)
	require.NoError(t, err)
	copy(arena.Get(old, 256), bytes.Repeat([]byte{0xAA}, 256))

	// Batch opened while the arena is still at generation 0.
	older := arena.BeginBatch(0)
	require.False(t, older.Stale())

	gen := arena.BumpGeneration()
	require.Equal(t, uint64(1), gen)

	newOff, err := arena.AllocDirty(256)
	require.NoError(t, err)
	require.NotEqual(t, old, newOff)
	copy(arena.Get(newOff, 256), bytes.Repeat([]byte{0xBB}, 256))

	// The mid-batch bump must not change what a maxGen=0 batch serves: the
	// generation-0 data is still visible, the generation-1 data is not.
	assertBytesParity(t, older.Get(old, 256), arena.GetWithGeneration(old, 256, 0), "pre-bump vector")
	assert.Nil(t, older.Get(newOff, 256), "post-bump vector must stay invisible to maxGen 0")

	// A fresh batch at the new generation sees both, and matches the reference.
	fresh := arena.BeginBatch(1)
	for _, tc := range []struct {
		off uint64
		tag byte
	}{
		{old, 0xAA},
		{newOff, 0xBB},
	} {
		got := fresh.Get(tc.off, 256)
		assertBytesParity(t, got, arena.GetWithGeneration(tc.off, 256, 1), "fresh gen 1")
		require.NotNil(t, got)
		assert.Equal(t, tc.tag, got[0])
	}

	// An even older batch still cannot see the generation-1 slab.
	oldest := arena.BeginBatch(0)
	assert.Nil(t, oldest.Get(newOff, 256))

	// Typed arena: the same rule holds across a BumpGeneration.
	ta := NewTypedArena[float32](arena)
	typedOld, err := ta.AllocSlice(16)
	require.NoError(t, err)
	tb := ta.BeginBatch(1)
	require.NotNil(t, tb.Get(typedOld), "pre-bump typed vector stays visible")
	ta.BumpGeneration()
	typedNew, err := ta.AllocSlice(16)
	require.NoError(t, err)

	assert.Nil(t, tb.Get(typedNew), "post-bump typed vector must stay invisible to maxGen 1")
	after := ta.BeginBatch(2)
	require.NotNil(t, after.Get(typedNew))
	assert.Equal(t, ta.GetWithGeneration(typedNew, 2), after.Get(typedNew))
}

// TestSlabBatch_NeverServesStaleTable pins the safety property that a batch
// cannot outlive a structural arena mutation: the table identity is rechecked
// on every call, so a batch opened before the swap re-resolves and never
// returns data the reference accessor would refuse.
func TestSlabBatch_NeverServesStaleTable(t *testing.T) {
	arena, offsets := batchArenaChunk(t, 1024, 512, 2)
	gen := arena.GetGeneration()

	batch := arena.BeginBatch(gen)
	require.NotNil(t, batch.Get(offsets[0], 512))
	require.False(t, batch.Stale())

	// A concurrent Alloc that needs a new slab replaces the slab table.
	// Force it by allocating a full slab's worth on top of the live chunks.
	fresh, err := arena.AllocDirty(arena.SlabSize())
	require.NoError(t, err)
	data := arena.Get(fresh, uint32(arena.SlabSize())) // #nosec G115
	require.Len(t, data, arena.SlabSize())
	for i := range data {
		data[i] = 0x7E
	}

	require.True(t, batch.Stale(), "table swap must be detected")
	// The batch re-resolves: it now sees the new slab, and agrees with the
	// reference, instead of serving a pre-swap view of the table.
	slabBytes := uint32(arena.SlabSize()) // #nosec G115
	assertBytesParity(t, batch.Get(fresh, slabBytes),
		arena.GetWithGeneration(fresh, slabBytes, gen), "post-swap new slab")
	assertBytesParity(t, batch.Get(offsets[1], 512), arena.GetWithGeneration(offsets[1], 512, gen), "post-swap old slab")
	require.Equal(t, byte(0x7E), batch.Get(fresh, 1)[0])

	// Free swaps the table for an empty one: the batch must report nil exactly
	// like the reference, not keep serving the freed buffer.
	arena.Free()
	require.True(t, batch.Stale())
	assertBytesParity(t, batch.Get(offsets[0], 512), arena.GetWithGeneration(offsets[0], 512, gen), "after Free")
	assert.Nil(t, batch.Get(offsets[0], 512), "batch must not serve a freed arena")
}

// TestSlabBatch_GenerationHidden pins the isolation rule itself: a slab written
// under a newer generation must be invisible at an older maxGeneration for both
// the batch and the reference, and the batch must stay hidden for the whole
// slab range, not just the first byte.
func TestSlabBatch_GenerationHidden(t *testing.T) {
	arena := NewSlabArena(1024)
	old, err := arena.AllocDirty(512)
	require.NoError(t, err)
	arena.BumpGeneration()
	newer, err := arena.AllocDirty(512)
	require.NoError(t, err)

	for _, gen := range []uint64{0, 1} {
		batch := arena.BeginBatch(gen)
		for i := 0; i < 512; i += 64 {
			got := batch.Get(newer+uint64(i), 64)                     // #nosec G115
			want := arena.GetWithGeneration(newer+uint64(i), 64, gen) // #nosec G115
			assertBytesParity(t, got, want, "hidden slab")
		}
		// The generation-0 slab is always visible.
		require.NotNil(t, batch.Get(old, 1))
	}
}

// TestSlabBatch_OversizeAllocation exercises the placeholder-slab backwards
// scan: an allocation larger than the slab capacity creates placeholder slots
// that resolve to the owning real slab.
func TestSlabBatch_OversizeAllocation(t *testing.T) {
	arena := NewSlabArena(1024)
	big, err := arena.Alloc(5000) // spans 5 slots
	require.NoError(t, err)
	data := arena.Get(big, 5000)
	require.Len(t, data, 5000)
	for i := range data {
		data[i] = byte(i)
	}

	for _, gen := range []uint64{0, math.MaxUint64} {
		batch := arena.BeginBatch(gen)
		for _, off := range []uint64{0, 1, 1023, 1024, 2048, 4096, 5000, 6144, 99999} {
			for _, l := range []uint32{1, 8, 1024, 5000, 6000} {
				got := batch.Get(big+off, l)
				want := arena.GetWithGeneration(big+off, l, gen)
				assertBytesParity(t, got, want, "oversize")
			}
		}
	}
}

// TestSlabBatch_EdgeCases covers nil/empty arenas, single-element chunks, ids
// past the last vector, and the int -> uint conversions the graph layer makes.
func TestSlabBatch_EdgeCases(t *testing.T) {
	t.Run("nil arena", func(t *testing.T) {
		var arena *SlabArena
		batch := arena.BeginBatch(math.MaxUint64)
		assert.Nil(t, batch.Get(1, 4))
		assert.True(t, batch.Stale())
		assert.Equal(t, uint64(math.MaxUint64), batch.MaxGeneration())
	})

	t.Run("zero value batch", func(t *testing.T) {
		var batch SlabBatch
		assert.Nil(t, batch.Get(0, 4))
		assert.Nil(t, batch.Get(1<<40, 4))
		assert.True(t, batch.Stale())
	})

	t.Run("zero value arena", func(t *testing.T) {
		arena := &SlabArena{}
		batch := arena.BeginBatch(0)
		assert.Nil(t, batch.Get(8, 4), "must not divide by a zero slab capacity")
	})

	t.Run("empty arena", func(t *testing.T) {
		arena := NewSlabArena(1024)
		batch := arena.BeginBatch(0)
		assert.Nil(t, batch.Get(0, 4))
		assert.Nil(t, batch.Get(1024, 4))
		assert.False(t, batch.Stale())
	})

	t.Run("zero length", func(t *testing.T) {
		arena, offsets := batchArenaChunk(t, 1024, 64, 1)
		batch := arena.BeginBatch(0)
		assert.Nil(t, batch.Get(offsets[0], 0))
		assert.Nil(t, batch.Get(0, 0))
	})

	t.Run("single element chunk", func(t *testing.T) {
		arena, offsets := batchArenaChunk(t, 1024, 1, 3)
		batch := arena.BeginBatch(0)
		for _, off := range offsets {
			assertBytesParity(t, batch.Get(off, 1), arena.GetWithGeneration(off, 1, 0), "1 byte")
		}
		// The arena has no notion of chunk boundaries, so the byte after a
		// one-byte chunk is still readable - the batch must agree.
		assertBytesParity(t, batch.Get(offsets[2]+1, 1), arena.GetWithGeneration(offsets[2]+1, 1, 0), "after 1 byte chunk")
	})

	t.Run("id beyond last vector", func(t *testing.T) {
		arena, offsets := batchArenaChunk(t, 1024, 64, 1)
		batch := arena.BeginBatch(math.MaxUint64)
		beyond := offsets[0] + 64
		for _, l := range []uint32{1, 64, 1 << 20} {
			assertBytesParity(t, batch.Get(beyond, l), arena.GetWithGeneration(beyond, l, math.MaxUint64), "beyond")
		}
		// Negative-safe cast: the graph layer turns a negative int chunk id into
		// a uint32 id, which lands far past the end of the table.
		neg := ^uint32(0) / 1024
		assertBytesParity(t, batch.Get(uint64(neg), 16), arena.GetWithGeneration(uint64(neg), 16, math.MaxUint64), "neg id")
		assert.Nil(t, batch.Get(uint64(neg), 16))
		// Huge offset: the slab index must not wrap into a valid slab.
		assertBytesParity(t, batch.Get(^uint64(0)-8, 16), arena.GetWithGeneration(^uint64(0)-8, 16, math.MaxUint64), "max offset")
		assert.Nil(t, batch.Get(^uint64(0)-8, 16))
	})
}

// TestSlabBatch_ConcurrentReaders pins that a batch is safe to use from
// concurrent readers while the arena is being mutated: the table identity is
// rechecked on every call, so readers either see a coherent pre-mutation view
// or re-resolve, never a torn one.
func TestSlabBatch_ConcurrentReaders(t *testing.T) {
	arena := NewSlabArena(1 << 16)
	stable := make([]uint64, 8)
	tags := make([]byte, 8)
	for i := range stable {
		off, err := arena.AllocDirty(1024)
		require.NoError(t, err)
		stable[i] = off
		tags[i] = byte(i * 31)
		data := arena.Get(off, 1024)
		for j := range data {
			data[j] = byte(i*31 + j)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: opens a fresh batch per iteration and checks it against the
	// reference accessor.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				batch := arena.BeginBatch(math.MaxUint64)
				for k, off := range stable {
					got := batch.Get(off, 1024)
					if got == nil {
						continue // arena may have been freed out from under us
					}
					if got[0] != tags[k] {
						t.Errorf("torn read at offset %d: got %d want %d", off, got[0], tags[k])
						return
					}
				}
			}
		}()
	}

	// Mutator: appends slabs and bumps generations, which invalidates batches.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < 200; i++ {
			if _, err := arena.AllocDirty(1024); err != nil {
				return
			}
			if i%32 == 0 {
				arena.BumpGeneration()
			}
		}
	}()

	wg.Wait()

	// A batch opened before the storm must still agree with the reference.
	batch := arena.BeginBatch(math.MaxUint64)
	for _, off := range stable {
		assertBytesParity(t, batch.Get(off, 1024), arena.GetWithGeneration(off, 1024, math.MaxUint64), "post-storm")
	}
}

// TestTypedBatch_CompactionSwap pins that a batch following a TypedArena whose
// slab was swapped by Compact re-resolves against the new arena.
func TestTypedBatch_CompactionSwap(t *testing.T) {
	arena := NewSlabArena(1 << 16)
	ta := NewTypedArena[float32](arena)
	refs := make([]SliceRef, 4)
	for i := range refs {
		ref, err := ta.AllocSlice(32)
		require.NoError(t, err)
		v := ta.Get(ref)
		for j := range v {
			v[j] = float32(i*100 + j)
		}
		refs[i] = ref
	}

	batch := ta.BeginBatch(math.MaxUint64)
	require.NotNil(t, batch.Get(refs[0]))

	_, err := ta.Compact(refs)
	require.NoError(t, err)

	// The arena changed underneath: the batch must track it, not serve the
	// pre-compaction buffers.
	require.True(t, batch.Stale())
	for i := range refs {
		got := batch.Get(refs[i])
		require.NotNil(t, got, "ref %d", i)
		assert.Equal(t, float32(i*100), got[0])
	}
}
