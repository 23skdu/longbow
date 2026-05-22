package memory

import (
	"math"
	"testing"
)

func FuzzArenaGetWithGeneration(f *testing.F) {
	f.Add(uint64(0), uint32(32), uint64(math.MaxUint64))
	f.Add(uint64(0), uint32(64), uint64(0))
	f.Add(uint64(100), uint32(16), uint64(1))
	f.Add(uint64(200), uint32(8), uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, offset uint64, length uint32, maxGen uint64) {
		arena := NewSlabArena(4096)

		allocOffset, err := arena.Alloc(256)
		if err != nil {
			return
		}

		// Fill with pattern
		data := arena.Get(allocOffset, 256)
		for i := range data {
			data[i] = byte(i)
		}

		// Read back with GetWithGeneration
		result := arena.GetWithGeneration(allocOffset, 256, maxGen)
		if len(result) > 0 {
			for i := range result {
				if result[i] != byte(i) {
					t.Fatalf("data mismatch at %d: got %d, want %d", i, result[i], byte(i))
				}
			}
		}

		// Verify MaxUint64 always returns data
		resultMax := arena.GetWithGeneration(allocOffset, 256, math.MaxUint64)
		if len(resultMax) == 0 {
			t.Error("GetWithGeneration with MaxUint64 should return data")
		}
	})
}
