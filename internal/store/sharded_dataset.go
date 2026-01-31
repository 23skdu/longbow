package store

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// =============================================================================
// ShardedDataset - Subtask 3: Dataset with Sharded Locking
// =============================================================================

// ShardedDatasetConfig configures a sharded dataset.
type ShardedDatasetConfig struct {
	NumShards int // Number of shards (default: runtime.NumCPU())
}

// DefaultShardedDatasetConfig returns sensible defaults.
func DefaultShardedDatasetConfig() ShardedDatasetConfig {
	return ShardedDatasetConfig{
		NumShards: runtime.NumCPU(),
	}
}

// Validate checks configuration validity.
func (c ShardedDatasetConfig) Validate() error {
	if c.NumShards <= 0 {
		return errors.New("NumShards must be positive")
	}
	return nil
}

// ShardedDatasetStats holds statistics for a sharded dataset.
type ShardedDatasetStats struct {
	TotalRecords    int
	NumShards       int
	RecordsPerShard []int
}

// ShardedDataset provides a dataset with sharded record storage.
// Enables concurrent access to different shards without lock contention.
type ShardedDataset struct {
	name       string
	records    *PartitionedRecords
	lastAccess int64 // UnixNano, atomic
	version    int64 // atomic
	index      VectorIndex
}

// NewShardedDataset creates a new sharded dataset with given config.
func NewShardedDataset(name string, cfg ShardedDatasetConfig) *ShardedDataset {
	if cfg.NumShards <= 0 {
		cfg.NumShards = runtime.NumCPU()
	}
	return &ShardedDataset{
		name:    name,
		records: NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: cfg.NumShards}),
	}
}

// NewShardedDatasetDefault creates a sharded dataset with default config.
func NewShardedDatasetDefault(name string) *ShardedDataset {
	return NewShardedDataset(name, DefaultShardedDatasetConfig())
}

// Name returns the dataset name.
func (sd *ShardedDataset) Name() string {
	return sd.name
}

// NumShards returns the number of shards.
func (sd *ShardedDataset) NumShards() int {
	return sd.records.NumPartitions()
}

// TotalRecords returns total number of record batches across all shards.
func (sd *ShardedDataset) TotalRecords() int {
	return sd.records.TotalBatches()
}

// ShardRecordCount returns the number of records in a specific shard.
func (sd *ShardedDataset) ShardRecordCount(shardIdx int) int {
	return sd.records.PartitionBatches(shardIdx)
}

// LastAccess returns the last access time.
func (sd *ShardedDataset) LastAccess() time.Time {
	return time.Unix(0, atomic.LoadInt64(&sd.lastAccess))
}

// SetLastAccess sets the last access time.
func (sd *ShardedDataset) SetLastAccess(t time.Time) {
	atomic.StoreInt64(&sd.lastAccess, t.UnixNano())
}

// Version returns the current version.
func (sd *ShardedDataset) Version() int64 {
	return atomic.LoadInt64(&sd.version)
}

// IncrementVersion atomically increments and returns the new version.
func (sd *ShardedDataset) IncrementVersion() int64 {
	return atomic.AddInt64(&sd.version, 1)
}

// Index returns the HNSW index (may be nil).
func (sd *ShardedDataset) Index() VectorIndex {
	return sd.index
}

// SetIndex sets the HNSW index.
func (sd *ShardedDataset) SetIndex(idx VectorIndex) {
	sd.index = idx
}

// Append adds a record batch using hash-based routing.
func (sd *ShardedDataset) Append(batch arrow.RecordBatch, routingKey uint64) {
	sd.records.AppendWithKey(batch, routingKey)
	sd.SetLastAccess(time.Now())
}

// AppendToShard adds a record batch directly to a specific shard.
func (sd *ShardedDataset) AppendToShard(batch arrow.RecordBatch, shardIdx int) {
	sd.records.AppendToPartition(batch, shardIdx)
	sd.SetLastAccess(time.Now())
}

// GetAllRecords returns all record batches across all shards.
// Order is not guaranteed to be consistent.
func (sd *ShardedDataset) GetAllRecords() []arrow.RecordBatch {
	sd.SetLastAccess(time.Now())
	return sd.records.GetAll()
}

// GetShardRecords returns all records from a specific shard.
func (sd *ShardedDataset) GetShardRecords(shardIdx int) []arrow.RecordBatch {
	sd.SetLastAccess(time.Now())
	return sd.records.GetPartition(shardIdx)
}

// ForEach iterates over all record batches.
// If fn returns false, iteration stops early.
func (sd *ShardedDataset) ForEach(fn func(batch arrow.RecordBatch, shard int) bool) {
	sd.records.ForEach(fn)
}

// ForEachInShard iterates over records in a specific shard.
func (sd *ShardedDataset) ForEachInShard(shardIdx int, fn func(batch arrow.RecordBatch) bool) {
	sd.records.ForEachInPartition(shardIdx, fn)
}

// ToLegacyRecords returns all records as a flat slice for backward compatibility.
func (sd *ShardedDataset) ToLegacyRecords() []arrow.RecordBatch {
	return sd.GetAllRecords()
}

// Stats returns current statistics.
func (sd *ShardedDataset) Stats() ShardedDatasetStats {
	prStats := sd.records.Stats()
	return ShardedDatasetStats{
		TotalRecords:    prStats.TotalBatches,
		NumShards:       prStats.NumPartitions,
		RecordsPerShard: prStats.BatchesPerPart,
	}
}

// Clear removes all records from all shards.
func (sd *ShardedDataset) Clear() {
	sd.records.Clear()
}

// ReplaceShardRecords atomically replaces all records in a shard.
func (sd *ShardedDataset) ReplaceShardRecords(shardIdx int, batches []arrow.RecordBatch) {
	sd.records.ReplaceAll(shardIdx, batches)
	sd.IncrementVersion()
}
