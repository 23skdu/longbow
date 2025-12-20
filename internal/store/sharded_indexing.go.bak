package store

import (
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
)

// ShardStats holds per-shard statistics
type ShardStats struct {
	ShardID       int
	JobsSent      int64
	QueueLength   int
	QueueCapacity int
}

// ShardedIndexChannel provides hash-based routing of IndexJobs
// to multiple channels, preventing noisy neighbor issues where
// one dataset's high write throughput blocks indexing for others.
type ShardedIndexChannel struct {
	shards     []chan IndexJob
	numShards  int
	bufferSize int
	jobsSent   []atomic.Int64 // per-shard counters
	closed     atomic.Bool
	closeOnce  sync.Once
}

// NewShardedIndexChannel creates a new sharded index channel
// with the specified number of shards and buffer size per shard.
func NewShardedIndexChannel(numShards, bufferSize int) *ShardedIndexChannel {
	if numShards < 1 {
		numShards = 1
	}
	if bufferSize < 1 {
		bufferSize = 1
	}

	sic := &ShardedIndexChannel{
		shards:     make([]chan IndexJob, numShards),
		numShards:  numShards,
		bufferSize: bufferSize,
		jobsSent:   make([]atomic.Int64, numShards),
	}

	for i := 0; i < numShards; i++ {
		sic.shards[i] = make(chan IndexJob, bufferSize)
	}

	return sic
}

// NewShardedIndexChannelDefault creates a sharded index channel
// with one shard per CPU core (recommended default).
func NewShardedIndexChannelDefault(bufferSize int) *ShardedIndexChannel {
	return NewShardedIndexChannel(runtime.NumCPU(), bufferSize)
}

// NumShards returns the number of shards.
func (sic *ShardedIndexChannel) NumShards() int {
	return sic.numShards
}

// GetShardChannel returns the channel for a specific shard ID.
// Used by workers to consume jobs from their assigned shard.
func (sic *ShardedIndexChannel) GetShardChannel(shardID int) chan IndexJob {
	if shardID < 0 || shardID >= sic.numShards {
		return nil
	}
	return sic.shards[shardID]
}

// GetShardForDataset returns the shard ID for a given dataset name.
// Uses FNV-1a hash for consistent, fast routing.
func (sic *ShardedIndexChannel) GetShardForDataset(datasetName string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(datasetName))
	return int(h.Sum32()) % sic.numShards
}

// Send routes an IndexJob to the appropriate shard based on dataset name.
// Blocks if the target shard's buffer is full.
// Returns false if the channel is closed.
func (sic *ShardedIndexChannel) Send(job IndexJob) bool {
	if sic.closed.Load() {
		return false
	}

	shardID := sic.GetShardForDataset(job.DatasetName)

	// Use defer/recover to handle send on closed channel
	defer func() {
		_ = recover()
	}()

	sic.shards[shardID] <- job
	sic.jobsSent[shardID].Add(1)
	return true
}

// TrySend attempts to send an IndexJob without blocking.
// Returns true if sent successfully, false if buffer full or closed.
func (sic *ShardedIndexChannel) TrySend(job IndexJob) bool {
	if sic.closed.Load() {
		return false
	}

	shardID := sic.GetShardForDataset(job.DatasetName)

	select {
	case sic.shards[shardID] <- job:
		sic.jobsSent[shardID].Add(1)
		return true
	default:
		return false
	}
}

// Close closes all shard channels. Safe to call multiple times.
func (sic *ShardedIndexChannel) Close() {
	sic.closeOnce.Do(func() {
		sic.closed.Store(true)
		for i := 0; i < sic.numShards; i++ {
			close(sic.shards[i])
		}
	})
}

// Stats returns statistics for all shards.
func (sic *ShardedIndexChannel) Stats() []ShardStats {
	stats := make([]ShardStats, sic.numShards)
	for i := 0; i < sic.numShards; i++ {
		stats[i] = ShardStats{
			ShardID:       i,
			JobsSent:      sic.jobsSent[i].Load(),
			QueueLength:   len(sic.shards[i]),
			QueueCapacity: cap(sic.shards[i]),
		}
	}
	return stats
}
