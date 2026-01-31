package store

import (
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
)

func FuzzPackedAdjacency(f *testing.F) {
	f.Add(uint32(10), uint32(5))
	f.Fuzz(func(t *testing.T, count uint32, seed uint32) {
		if count > 50 {
			count = 50
		}
		if count == 0 {
			count = 1
		}

		arena := memory.NewSlabArena(1024 * 1024)
		adj := NewPackedAdjacency(arena, int(count))

		rnd := rand.New(rand.NewSource(int64(seed)))

		data := make(map[uint32][]uint32)

		// Fill
		for i := uint32(0); i < count; i++ {
			adj.EnsureCapacity(i)

			// Random neighbors
			numNbrs := rnd.Intn(20)
			nbrs := make([]uint32, numNbrs)
			for j := 0; j < numNbrs; j++ {
				nbrs[j] = rnd.Uint32()
			}

			err := adj.SetNeighbors(i, nbrs)
			if err != nil {
				t.Fatalf("SetNeighbors failed: %v", err)
			}
			if len(nbrs) == 0 {
				nbrs = nil
			}
			data[i] = nbrs
		}

		// Verify
		for i := uint32(0); i < count; i++ {
			got, ok := adj.GetNeighbors(i)
			if !ok {
				t.Fatalf("Missing neighbors for %d", i)
			}
			assert.Equal(t, data[i], got)
		}
	})
}
