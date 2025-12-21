package store

import (
	"fmt"
	"runtime"

	"github.com/23skdu/longbow/internal/metrics"
)

// AutoShardingConfig configures automatic HNSW index sharding
type AutoShardingConfig struct {
	Enabled        bool
	Threshold      int
	NumShards      int
	M              int
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

// MigrateToShardedInDataset performs an in-place migration of a dataset's index.
func MigrateToShardedInDataset(d *Dataset, cfg AutoShardingConfig) error {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()

	if _, ok := d.Index.(*ShardedHNSW); ok {
		return nil // Already sharded
	}

	hnswIdx, ok := d.Index.(*HNSWIndex)
	if !ok {
		return fmt.Errorf("current index is not HNSW, cannot migrate")
	}

	sharded, err := MigrateToSharded(hnswIdx, cfg)
	if err != nil {
		return err
	}

	d.Index = sharded
	return nil
}

// ShouldShard returns true if the vector count exceeds the threshold.
func (c AutoShardingConfig) ShouldShard(vectorCount int) bool {

	if !c.Enabled {
		return false
	}
	return vectorCount >= c.Threshold
}

// MigrateToSharded converts an HNSWIndex to a ShardedHNSW.
func MigrateToSharded(original *HNSWIndex, cfg AutoShardingConfig) (*ShardedHNSW, error) {
	if original == nil {
		return nil, fmt.Errorf("original index is nil")
	}

	ds := original.dataset
	shardConfig := ShardedHNSWConfig{
		NumShards:      cfg.NumShards,
		M:              cfg.M,
		EfConstruction: cfg.EfConstruction,
		Metric:         original.Metric,
	}

	sharded := NewShardedHNSW(shardConfig, ds)
	origLen := original.Len()
	if origLen == 0 {
		return sharded, nil
	}

	// Migrate vectors
	for i := 0; i < origLen; i++ {
		vid := VectorID(i)
		vec := original.getVector(vid)
		if vec == nil {
			continue
		}
		loc, _ := original.GetLocation(vid)
		if err := sharded.AddByRecord(nil, loc.RowIdx, loc.BatchIdx); err != nil {
			// Note: AddByRecord in ShardedHNSW (as restored) expects a record or uses dataset.
			// Let's use AddByLocation if dataset is linked.
			if err := sharded.AddByLocation(loc.BatchIdx, loc.RowIdx); err != nil {
				return nil, err
			}
		}
	}

	metrics.HnswShardingMigrationsTotal.Inc()
	return sharded, nil
}
