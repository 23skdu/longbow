package store

import (
	"math"
	"math/rand"
	"sync"
)

// LevelGenerator generates random levels for HNSW nodes using exponential decay.
// This ensures higher layers have exponentially fewer nodes, creating a hierarchical structure.
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
		rng: rand.New(rand.NewSource(42)), // Fixed seed for reproducibility
	}
}

// Generate returns a random level using exponential decay.
// Uses the formula: level = -ln(uniform(0,1)) * ml
// The level is capped at ArrowMaxLayers - 1 to prevent excessive memory usage.
func (lg *LevelGenerator) Generate() int {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// Use exponential distribution: -ln(uniform(0,1)) * ml
	uniform := lg.rng.Float64()
	level := int(-math.Log(uniform) * lg.ml)

	// Cap at ArrowMaxLayers - 1
	if level >= ArrowMaxLayers {
		level = ArrowMaxLayers - 1
	}

	return level
}
