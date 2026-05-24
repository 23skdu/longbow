package store

import (
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/memory"
)

func FuzzArenaVectorConcurrentAlloc(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping fuzz test in short mode")
	}
	fuzzDims := []int{64, 128, 256, 512, 1024}
	fuzzConcurrency := []int{1, 2, 4, 8, 16}

	for _, dim := range fuzzDims {
		for _, conc := range fuzzConcurrency {
			f.Logf("Testing dim=%d, concurrency=%d", dim, conc)
		}
	}

	f.Fuzz(func(t *testing.T, seed int64, allocCount int) {
		if allocCount <= 0 || allocCount > 10000 {
			t.Skip()
		}

		dim := fuzzDims[seed%int64(len(fuzzDims))]
		arena := memory.NewTypedArena[float32](memory.NewSlabArena(1024 * 1024))

		var wg sync.WaitGroup
		vecs := make([]memory.SliceRef, 0, allocCount)
		mu := sync.Mutex{}

		conc := int(fuzzConcurrency[seed%int64(len(fuzzConcurrency))])
		chunks := allocCount / conc
		remainder := allocCount % conc

		for i := 0; i < conc; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				localVecs := make([]memory.SliceRef, 0, chunks+1)
				count := chunks
				if workerID < remainder {
					count++
				}

				for j := 0; j < count; j++ {
					ref, err := arena.AllocSlice(dim)
					if err != nil {
						t.Errorf("Worker %d: AllocSlice failed: %v", workerID, err)
						return
					}
					localVecs = append(localVecs, ref)
				}

				mu.Lock()
				vecs = append(vecs, localVecs...)
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		if len(vecs) != allocCount {
			t.Errorf("Expected %d allocations, got %d", allocCount, len(vecs))
		}

		for i, ref := range vecs {
			slice := arena.Get(ref)
			if slice == nil || len(slice) != dim {
				t.Errorf("Vec %d: invalid slice (len=%d, want=%d)", i, len(slice), dim)
			}
		}
	})
}

func TestArenaVectorConcurrentAlloc(t *testing.T) {
	if testing.Short() {
			t.Skip("skipping test in short mode")
	}
	arena := memory.NewTypedArena[float32](memory.NewSlabArena(1024 * 1024))
	dim := 128
	conc := 8
	iterations := 100

	var wg sync.WaitGroup
	vecs := make([]memory.SliceRef, conc)
	mu := sync.Mutex{}

	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				ref, err := arena.AllocSlice(dim)
				if err != nil {
					t.Errorf("Worker %d: AllocSlice failed: %v", workerID, err)
					return
				}
				slice := arena.Get(ref)
				if slice == nil || len(slice) != dim {
					t.Errorf("Worker %d: invalid slice at iter %d", workerID, j)
					return
				}
				mu.Lock()
				vecs[workerID] = ref
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	arenaStats := arena.Slab().Stats()
	t.Logf("Arena stats: TotalCapacity=%d, UsedBytes=%d",
		arenaStats.TotalCapacity, arenaStats.UsedBytes)
}

func BenchmarkArena_VectorStorage(b *testing.B) {
	arena := memory.NewTypedArena[float32](memory.NewSlabArena(64 * 1024 * 1024))
	dim := 128
	vecCount := 10000

	vecs := make([]memory.SliceRef, vecCount)
	for i := 0; i < vecCount; i++ {
		ref, err := arena.AllocSlice(dim)
		if err != nil {
			b.Fatalf("AllocSlice failed: %v", err)
		}
		vecs[i] = ref
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, ref := range vecs {
			slice := arena.Get(ref)
			_ = slice[0]
		}
	}
}

func BenchmarkArena_VectorStorageVsMap(b *testing.B) {
	dim := 128
	vecCount := 10000

	b.Run("Arena", func(b *testing.B) {
		arena := memory.NewTypedArena[float32](memory.NewSlabArena(64 * 1024 * 1024))
		vecs := make([]memory.SliceRef, vecCount)

		for i := 0; i < vecCount; i++ {
			ref, _ := arena.AllocSlice(dim)
			vecs[i] = ref
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			for _, ref := range vecs {
				slice := arena.Get(ref)
				_ = slice[0]
			}
		}
	})

	b.Run("Map", func(b *testing.B) {
		vecs := make(map[int][]float32, vecCount)

		for i := 0; i < vecCount; i++ {
			vec := make([]float32, dim)
			vecs[i] = vec
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			for j := 0; j < vecCount; j++ {
				vec := vecs[j]
				_ = vec[0]
			}
		}
	})
}
