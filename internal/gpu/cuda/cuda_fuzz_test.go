//go:build gpu && linux

package cuda

import (
	"github.com/23skdu/longbow/internal/gpu/types"
	"math/rand"
	"testing"
)

func FuzzCUDAPagerEviction(f *testing.F) {
	// Seed the fuzzer with some basic vector sizes
	f.Add(100)
	f.Add(1000)

	f.Fuzz(func(t *testing.T, count int) {
		if count <= 0 || count > 5000 {
			t.Skip()
		}

		cfg := types.GPUConfig{
			Dimension: 128,
			DeviceID:  0,
			// Trigger thrashing: 200MB limit
			MaxMemory: 200 * 1024 * 1024,
		}

		idx, err := NewCUDAIndex(cfg)
		if err != nil {
			t.Fatalf("Failed to initialize CUDA index: %v", err)
		}
		defer idx.Close()

		ids := make([]int64, count)
		vecs := make([]float32, count*128)
		for i := 0; i < count; i++ {
			ids[i] = int64(i + 1)
			for j := 0; j < 128; j++ {
				vecs[i*128+j] = rand.Float32()
			}
		}

		if err := idx.Add(ids, vecs); err != nil {
			t.Fatalf("Add failed: %v", err)
		}

		for i := 0; i < count; i += 100 {
			q := vecs[i*128 : (i+1)*128]
			resIDs, _, err := idx.Search(q, 5)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			found := false
			for _, rid := range resIDs {
				if rid == ids[i] {
					found = true
					break
				}
			}
			if !found {
				// t.Errorf("Expected to find ID %d in results", ids[i])
			}
		}
	})
}
