package index

// nosec G404 - math/rand is used for HNSW level generation, not security-sensitive
import (
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"github.com/23skdu/longbow/internal/store/types"
)

// LevelSeed seeds the HNSW level assignment, and with it the shape of every
// graph this process builds.
//
// Level assignment used to draw from the global math/rand source, which Go
// seeds from the clock since 1.20. Two builds of the same corpus in the same
// binary therefore differed in level assignment, and because a node's level
// decides which layers it is linked into, they differed in graph topology,
// in the entry point, and in search output. That made any A/B of a build
// change un-comparable (the recall spread this removed from the narrow-insert
// A/B in roadmap Item 29 was 0.380-0.428 on one test alone) and made the
// harness's own reproducibility claim unreachable: the client side was seeded
// from --seed and the server side was seeded from the clock.
//
// A fixed default keeps every build reproducible out of the box. Benchmarks
// override it with LONGBOW_BENCH_SEED so the same seed drives corpus
// generation, query generation and index construction (roadmap Item 31).
const defaultLevelSeed uint64 = 42

// LevelSeed is the seed for deterministic per-node level assignment. Read once
// from LONGBOW_BENCH_SEED at package init; see defaultLevelSeed for why it is
// deterministic at all.
var LevelSeed uint64 = func() uint64 {
	if v := os.Getenv("LONGBOW_BENCH_SEED"); v != "" {
		if s, err := strconv.ParseInt(v, 10, 64); err == nil {
			return uint64(s) // #nosec G115 -- a negative seed is just another key
		}
	}
	return defaultLevelSeed
}()

// LevelUnit maps (seed, node ID) onto a value in (0,1] with a splitmix64
// finalizer. It is a pure function of its arguments, which is the whole point:
// the level a node gets must not depend on the order the linking goroutines
// happen to reach it in.
//
// The result is biased away from zero deliberately. The caller takes -log of
// it, and -log(0) is +Inf, which converts to an implementation-defined int.
// Drawing from (0,1] instead keeps the exponential distribution exact and
// removes the special case.
func LevelUnit(seed uint64, id uint32) float64 {
	x := uint64(id) ^ (seed * 0x9E3779B97F4A7C15)
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	// 53 significant bits, exactly representable in a float64 mantissa.
	return (float64(x>>11) + 1.0) * (1.0 / float64(uint64(1)<<53))
}

// levelFor returns the level a node is assigned, from LevelSeed and the node's
// own ID: the standard HNSW exponential decay, level = -ln(u) * levelMultiplier
// capped at types.ArrowMaxLayers-1, with u drawn from LevelUnit instead of from
// a process-global RNG. Same (seed, id) -> same level, in any process, on any
// machine, in any insert order.
func (h *ArrowHNSW) levelFor(id uint32) int {
	l := int(math.Floor(-math.Log(LevelUnit(LevelSeed, id)) * h.levelMultiplier))
	if l < 0 || l >= types.ArrowMaxLayers {
		l = types.ArrowMaxLayers - 1
	}
	return l
}

// LevelGenerator generates random levels for HNSW nodes using exponential decay.
// This ensures higher layers have exponentially fewer nodes, creating a hierarchical structure.
//
// It is no longer on any insert path: every node that enters a graph gets its
// level from levelFor, which is a function of the seed and the node ID rather
// than of a draw order. What is left is the tests that drive level assignment
// directly, and their seed is fixed at 42 so those are reproducible too.
type LevelGenerator struct {
	ml  float64
	rng *rand.Rand
	mu  sync.Mutex
}

// NewLevelGenerator creates a new level generator with the specified ml parameter.
// ml controls the decay rate - higher ml means nodes reach higher levels more frequently.
func NewLevelGenerator(ml float64) *LevelGenerator {
	return &LevelGenerator{
		ml:  ml,
		rng: rand.New(rand.NewSource(42)), // #nosec G404
	}
}

// Generate returns a random level using exponential decay.
// Uses the formula: level = -ln(uniform(0,1)) * ml
// The level is capped at types.ArrowMaxLayers - 1 to prevent excessive memory usage.
func (lg *LevelGenerator) Generate() int {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// Use exponential distribution: -ln(uniform(0,1)) * ml
	uniform := lg.rng.Float64()
	level := int(-math.Log(uniform) * lg.ml)

	// Cap at types.ArrowMaxLayers - 1
	if level >= types.ArrowMaxLayers {
		level = types.ArrowMaxLayers - 1
	}

	return level
}
