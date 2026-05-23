package memory

import (
	"testing"
)

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
