package index

import (
	"context"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

// TestUint8GraphReachabilityAcrossDistributions pins the selectNeighbors uint8
// branch across more than one synthetic corpus. The branch landed in this
// session (selectNeighbors now has a VectorTypeUint8 case keyed on the native
// 8-bit distance kernel); on a single clustered corpus uint8 reached 4000/4000
// like every other dtype. A single corpus is not enough evidence against a
// future regressions: the original failure occurred on a 3-cluster corpus but
// was invisible on the 40-cluster corpus that the recall test used.
//
// Here the index is built on five deliberately different shapes:
//  1. tight clusters (3 clusters, low spread) -- the shape that surfaced the bug
//  2. wide uniform random vectors in [-1,1] -- the worst case for 8-bit
//  3. 12 overlapping clusters (medium spread)
//  4. a single dense hub (one cluster, tight)
//  5. a sparse/bimodal corpus (components near 0 or 1)
//
// Target: each index reaches at least 95% of its 4000 nodes from the entry point
// at efSearch=500. If any distribution falls below that, the diversity heuristic
// has been bypassed or the uint8 path has regressed.
func TestUint8GraphReachabilityAcrossDistributions(t *testing.T) {
	if testing.Short() {
		t.Skip("builds five 4k indexes")
	}
	const dims, n = 128, 4000

	gens := []struct {
		name string
		corp [][]float32
	}{
		{"tight_clusters", clusterCorpus(t, 3, n, dims, 3, 0.04, 0.8, 1.4)},
		{"uniform", func() [][]float32 {
			rng := rand.New(rand.NewSource(9)) // #nosec G404
			out := make([][]float32, n)
			for i := range out {
				v := make([]float32, dims)
				for j := range v {
					v[j] = rng.Float32()*2 - 1
				}
				out[i] = v
			}
			return out
		}()},
		{"twelve_clusters", clusterCorpus(t, 17, n, dims, 12, 0.12, 0.6, 1.3)},
		{"single_hub", clusterCorpus(t, 42, n, dims, 1, 0.03, 0.9, 0.9)},
		{"sparse_bimodal", func() [][]float32 {
			rng := rand.New(rand.NewSource(7)) // #nosec G404
			out := make([][]float32, n)
			for i := range out {
				v := make([]float32, dims)
				for j := range v {
					if rng.Float32() > 0.7 {
						v[j] = 1.0
					} else {
						v[j] = -1.0 + rng.Float32()*0.2
					}
				}
				out[i] = v
			}
			return out
		}()},
	}

	for _, g := range gens {
		g := g
		t.Run(g.name, func(t *testing.T) {
			idx, stored := buildQuantizedCorpus(t, g.corp, arrow.PrimitiveTypes.Uint8, types.VectorTypeUint8)
			res, err := idx.SearchVectors(context.Background(), stored[776], n, nil, types.SearchOptions{Ef: 500})
			if err != nil {
				t.Fatal(err)
			}
			reach := float64(len(res)) / float64(n)
			t.Logf("UINT8_REACH %-20s %d/%d (%.1f%%)", g.name, len(res), n, reach*100)
			if reach < 0.95 {
				t.Errorf("uint8 reachability on %q is %.1f%%, below the 95%% bar", g.name, reach*100)
			}
		})
	}
}
