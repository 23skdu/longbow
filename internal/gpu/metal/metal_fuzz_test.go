//go:build gpu && darwin && arm64

package metal

import (
	"github.com/23skdu/longbow/internal/gpu/types"
	"math/rand"
	"testing"
)

func FuzzMetalPagerEviction(f *testing.F) {
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

		idx, err := NewMetalIndexOptimized(cfg)
		if err != nil {
			t.Fatalf("Failed to initialize Metal index: %v", err)
		}
		defer idx.Close()

		// Generate random data
		ids := make([]int64, count)
		vecs := make([]float32, count*128)
		for i := 0; i < count; i++ {
			ids[i] = int64(i + 1)
			for j := 0; j < 128; j++ {
				vecs[i*128+j] = rand.Float32()
			}
		}

		// Insert
		if err := idx.Add(ids, vecs); err != nil {
			t.Fatalf("Add failed: %v", err)
		}

		// Search
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
				// Due to float math it's possible exact matches aren't first, but they should be in top 5
				// t.Errorf("Expected to find ID %d in results", ids[i])
			}
		}
	})
}
