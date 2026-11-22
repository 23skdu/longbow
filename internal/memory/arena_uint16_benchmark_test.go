package memory

import (
	"testing"
)

// BenchmarkSlabArena_Uint16_Dim1024 runs the benchmark slab arena_uint16_dim1024 operation.
func BenchmarkSlabArena_Uint16_Dim1024(b *testing.B) {
	// uint16 dim=1024 is 2048 bytes
	size := 1024 * 2
	arena := NewSlabArena(100 * 1024 * 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := arena.Alloc(size)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSlabArena_Uint16_Dim3072 runs the benchmark slab arena_uint16_dim3072 operation.
func BenchmarkSlabArena_Uint16_Dim3072(b *testing.B) {
	// uint16 dim=3072 is 6144 bytes
	size := 3072 * 2
	arena := NewSlabArena(100 * 1024 * 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := arena.Alloc(size)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
