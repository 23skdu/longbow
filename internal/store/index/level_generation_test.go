package index

// Roadmap Item 31: the level a node is assigned is a function of the seed and
// the node's ID, and of nothing else.
//
// It used to be drawn from the process-global math/rand, which Go seeds from
// the clock. Every build of the same corpus therefore had a different level
// assignment, and since a node's level decides which layers it is linked into,
// a different graph, a different entry point and different search output. That
// is what made the harness's reproducibility claim unverifiable: the client
// side was seeded from --seed and the server side was seeded from the clock.

import (
	"math"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
)

func TestLevelForIsADeterministicFunctionOfNodeID(t *testing.T) {
	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = 4
	cfg.M = 16
	h := NewArrowHNSW(&MockDataset{}, &cfg, nil)

	const n = 4096
	baseline := make([]int, n)
	for id := uint32(0); id < n; id++ {
		baseline[id] = h.levelFor(id)
		// Repeat calls, because a first call that happens to be stable while
		// later ones drift (a cache, a counter, a shared RNG) would still be
		// nondeterministic in the way that matters.
		for c := 0; c < 3; c++ {
			if got := h.levelFor(id); got != baseline[id] {
				t.Fatalf("levelFor(%d) returned %d, then %d: the level must not depend on how many times it is asked",
					id, baseline[id], got)
			}
		}
		if baseline[id] < 0 || baseline[id] >= types.ArrowMaxLayers {
			t.Fatalf("levelFor(%d) = %d, outside [0, %d)", id, baseline[id], types.ArrowMaxLayers)
		}
	}

	// The decay is unchanged by making it deterministic: this is still
	// level = floor(-ln(u) * 1/ln(M)), so P(level >= l) = 1/M^l, and the mean
	// is the tail sum sum_{l>=1} 1/M^l = 1/(M-1).
	mean := 0.0
	atLeastOne := 0
	for _, l := range baseline {
		mean += float64(l)
		if l >= 1 {
			atLeastOne++
		}
	}
	mean /= float64(n)
	wantMean := 1.0 / float64(cfg.M-1)
	if mean < wantMean*0.75 || mean > wantMean*1.25 {
		t.Errorf("mean level %.4f, want about 1/(M-1) = %.4f: the distribution is not exponential decay any more",
			mean, wantMean)
	}
	pAtLeast1 := float64(atLeastOne) / float64(n)
	want := 1.0 / float64(cfg.M)
	if math.Abs(pAtLeast1-want) > want*0.25 {
		t.Errorf("P(level >= 1) = %.4f, want about 1/M = %.4f", pAtLeast1, want)
	}

	// A second seed has to produce a different assignment, or LevelSeed is
	// wired to nothing and the whole mechanism is decoration.
	saved := LevelSeed
	t.Cleanup(func() { LevelSeed = saved })
	LevelSeed = saved + 1

	differing := 0
	for id := uint32(0); id < n; id++ {
		if h.levelFor(id) != baseline[id] {
			differing++
		}
	}
	if differing == 0 {
		t.Error("changing LevelSeed changed no node's level: the seed does not reach levelFor")
	}
	if differing == n {
		t.Errorf("changing LevelSeed changed every node's level (%d/%d): the node ID is not in the mix, "+
			"so two indexes built in different orders would still differ everywhere", differing, n)
	}
}

func TestLevelUnitStaysInsideTheOpenInterval(t *testing.T) {
	// -log(0) is +Inf, and int(+Inf) is implementation-defined. The caller
	// would need a special case if LevelUnit could return 0.
	for id := uint32(0); id < 100000; id++ {
		u := LevelUnit(defaultLevelSeed, id)
		if u <= 0 || u > 1 {
			t.Fatalf("LevelUnit(%d) = %g, want (0, 1]", id, u)
		}
	}
}
