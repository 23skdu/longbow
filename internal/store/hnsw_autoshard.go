package store

import (
	"fmt"
	"runtime"

	"github.com/23skdu/longbow/internal/metrics"
)

// AutoShardingConfig configures automatic HNSW index sharding
// when vector count exceeds the threshold.
type AutoShardingConfig struct {
	// Enabled controls whether auto-sharding is active
	Enabled bool

	// Threshold is the vector count above which sharding triggers
	Threshold int

	// NumShards is the number of shards to create
	NumShards int

	// M is the HNSW M parameter (max connections per layer)
	M int

	// EfConstruction is the HNSW construction search depth
	EfConstruction int
}

// DefaultAutoShardingConfig returns sensible defaults for auto-sharding.
func DefaultAutoShardingConfig() AutoShardingConfig {
	return AutoShardingConfig{
		Enabled:        true,
		Threshold:      10000,
		NumShards:      runtime.NumCPU(),
		M:              16,
		EfConstruction: 200,
	}
}

// Validate checks the configuration for errors.
func (c AutoShardingConfig) Validate() error {
	if !c.Enabled {
		return nil // Skip validation when disabled
	}
	if c.Threshold <= 0 {
		return fmt.Errorf("threshold must be positive, got %d", c.Threshold)
	}
	if c.NumShards <= 0 {
		return fmt.Errorf("numShards must be positive, got %d", c.NumShards)
	}
	if c.M <= 0 {
		return fmt.Errorf("m parameter must be positive, got %d", c.M)
	}
	if c.EfConstruction <= 0 {
		return fmt.Errorf("efConstruction must be positive, got %d", c.EfConstruction)
	}
	return nil
}

// ShouldShard returns true if the vector count exceeds the threshold.
func (c AutoShardingConfig) ShouldShard(vectorCount int) bool {
	if !c.Enabled {
		return false
	}
	return vectorCount > c.Threshold
}

// MigrateToSharded converts an HNSWIndex to a ShardedHNSW.
// It extracts all vectors from the original index and redistributes
// them across shards using consistent hashing.
func MigrateToSharded(original *HNSWIndex, cfg AutoShardingConfig) (*ShardedHNSW, error) {
	if original == nil {
		return nil, fmt.Errorf("original index cannot be nil")
	}

	// Get the dataset from the original index
	ds := original.dataset

	// Create sharded config from auto-sharding config
	shardConfig := ShardedHNSWConfig{
		NumShards:      cfg.NumShards,
		M:              cfg.M,
		EfConstruction: cfg.EfConstruction,
	}

	// Create new sharded index
	sharded := NewShardedHNSW(shardConfig, ds)

	// If original is empty, just return the new sharded index
	origLen := original.Len()
	if origLen == 0 {
		return sharded, nil
	}

	// Migrate vectors from original to sharded
	err := migrateVectors(original, sharded, origLen)
	if err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	// Record metric
	metrics.HnswShardingMigrationsTotal.Inc()

	return sharded, nil
}

// migrateVectors transfers vectors from HNSWIndex to ShardedHNSW.
func migrateVectors(original *HNSWIndex, sharded *ShardedHNSW, count int) error {
	// Iterate through all VectorIDs
	for i := 0; i < count; i++ {
		vid := VectorID(i)

		// Get vector using VectorID
		vec := original.getVector(vid)
		if vec == nil {
			continue
		}

		// Get location from original index
		loc := original.locations[vid]

		// Add to sharded index
		_, err := sharded.AddVector(loc, vec)
		if err != nil {
			return err
		}
	}

	return nil
}
