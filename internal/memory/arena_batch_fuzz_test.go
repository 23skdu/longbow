package memory

import (
	"math"
	"testing"
)

// FuzzSlabBatch_Parity compares SlabBatch.Get against the reference
// SlabArena.GetWithGeneration for random arena layouts, chunk sizes, ids,
// lengths and generations.
func FuzzSlabBatch_Parity(f *testing.F) {
	f.Add(uint32(1024), uint32(64), uint8(3), uint64(0), uint64(0), uint16(0))
	f.Add(uint32(4096), uint32(4096), uint8(2), uint64(1), uint64(5), uint16(1))
	f.Add(uint32(1<<16), uint32(128), uint8(8), uint64(math.MaxUint64), uint64(3), uint16(64))
	f.Add(uint32(1024), uint32(1024), uint8(1), uint64(0), uint64(9), uint16(4095))

	f.Fuzz(func(t *testing.T, slabSize, chunkBytes uint32, nChunks uint8, gen, maxGen uint64, idSeed uint16) {
		if slabSize < 1024 || slabSize > 1<<22 {
			t.Skip()
		}
		if chunkBytes == 0 || chunkBytes > 1<<20 {
			t.Skip()
		}
		if nChunks == 0 {
			t.Skip()
		}
		// Keep the fuzz body small: at most 8 chunks of at most 64KiB.
		if nChunks > 8 || chunkBytes > 1<<16 {
			t.Skip()
		}

		arena := NewSlabArena(int(slabSize))
		if gen > 4 {
			arena.generation.Store(gen - 1)
		}
		offsets := make([]uint64, 0, nChunks)
		for i := 0; i < int(nChunks); i++ {
			off, err := arena.AllocDirty(int(chunkBytes))
			if err != nil {
				break
			}
			offsets = append(offsets, off)
			if i%3 == 0 {
				data := arena.Get(off, chunkBytes)
				for j := range data {
					data[j] = byte(i*13 + j*7)
				}
			}
			if i%4 == 2 {
				// Force a new slab so later chunks land in fresh tables.
				if _, err := arena.AllocDirty(int(slabSize)); err != nil {
					break
				}
			}
		}
		if len(offsets) == 0 {
			return
		}

		if maxGen == math.MaxUint64-3 {
			maxGen = math.MaxUint64
		}

		batch := arena.BeginBatch(maxGen)

		// Deterministic pseudo-random walk over ids and lengths, including
		// chunk boundaries, the last byte of a chunk and ids past the arena.
		state := uint64(idSeed)*6364136223846793005 + 1442695040888963407
		next := func(n uint64) uint64 {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			return state % n
		}

		for i := 0; i < 64; i++ {
			var off uint64
			switch i % 4 {
			case 0: // inside a chunk
				off = offsets[next(uint64(len(offsets)))] + next(uint64(chunkBytes))
			case 1: // first byte of a chunk
				off = offsets[next(uint64(len(offsets)))]
			case 2: // one past the end of a chunk
				off = offsets[next(uint64(len(offsets)))] + uint64(chunkBytes)
			default: // far past the arena
				off = uint64(next(1 << 40))
			}

			length := uint32(next(1<<12) + 1) // #nosec G115
			if i%5 == 0 {
				length = chunkBytes
			}
			if i%7 == 0 {
				length = 0
			}

			got := batch.Get(off, length)
			want := arena.GetWithGeneration(off, length, maxGen)
			if (got == nil) != (want == nil) {
				t.Fatalf("nil mismatch at i=%d off=%d len=%d maxGen=%d: batch=%v ref=%v",
					i, off, length, maxGen, got == nil, want == nil)
			}
			if got == nil {
				continue
			}
			if len(got) != len(want) {
				t.Fatalf("length mismatch at i=%d off=%d len=%d: batch=%d ref=%d", i, off, length, len(got), len(want))
			}
			if &got[0] != &want[0] {
				t.Fatalf("backing memory mismatch at i=%d off=%d len=%d", i, off, length)
			}
			for j := range got {
				if got[j] != want[j] {
					t.Fatalf("byte mismatch at i=%d off=%d len=%d index=%d: batch=%d ref=%d",
						i, off, length, j, got[j], want[j])
				}
			}
		}
	})
}

// FuzzTypedBatch_Parity is the typed-arena equivalent of FuzzSlabBatch_Parity.
func FuzzTypedBatch_Parity(f *testing.F) {
	f.Add(uint32(4096), uint32(16), uint8(4), uint64(0), uint64(1))
	f.Add(uint32(1<<16), uint32(128), uint8(2), uint64(2), uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, slabSize, elems uint32, nChunks uint8, gen, maxGen uint64) {
		if slabSize < 1024 || slabSize > 1<<20 {
			t.Skip()
		}
		if elems == 0 || elems > 4096 {
			t.Skip()
		}
		if nChunks == 0 || nChunks > 8 {
			t.Skip()
		}

		arena := NewSlabArena(int(slabSize))
		ta := NewTypedArena[float32](arena)
		if gen > 4 {
			ta.SetGeneration(gen - 1)
		}

		refs := make([]SliceRef, 0, nChunks)
		for i := 0; i < int(nChunks); i++ {
			ref, err := ta.AllocSlice(int(elems))
			if err != nil {
				break
			}
			refs = append(refs, ref)
			v := ta.Get(ref)
			for j := range v {
				v[j] = float32(i*31 + j)
			}
		}
		if len(refs) == 0 {
			return
		}

		if maxGen == math.MaxUint64-3 {
			maxGen = math.MaxUint64
		}

		batch := ta.BeginBatch(maxGen)
		elemBytes := uint32(4) * elems // #nosec G115

		for _, ref := range refs {
			cases := []SliceRef{
				ref,
				{Offset: ref.Offset, Len: 1, Cap: 1},
				{Offset: ref.Offset + uint64(elemBytes) - 4, Len: 1, Cap: 1}, // #nosec G115
				{Offset: ref.Offset + uint64(elemBytes), Len: 1, Cap: 1},     // #nosec G115
				{Offset: ref.Offset, Len: 0, Cap: 0},
			}
			for _, c := range cases {
				got := batch.Get(c)
				want := ta.GetWithGeneration(c, maxGen)
				if (got == nil) != (want == nil) {
					t.Fatalf("nil mismatch ref=%+v maxGen=%d: batch=%v ref=%v", c, maxGen, got == nil, want == nil)
				}
				if got == nil {
					continue
				}
				if len(got) != len(want) {
					t.Fatalf("length mismatch ref=%+v: batch=%d ref=%d", c, len(got), len(want))
				}
				for j := range got {
					if got[j] != want[j] {
						t.Fatalf("value mismatch ref=%+v index=%d: batch=%v ref=%v", c, j, got[j], want[j])
					}
				}
			}
		}
	})
}
