package store

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/temporal"
)

// SegmentNode represents a segment node value.
type SegmentNode = temporal.SegmentNode

// SegmentTree represents a segment tree value.
type SegmentTree = temporal.SegmentTree

// TemporalAggregator represents a temporal aggregator value.
type TemporalAggregator = temporal.TemporalAggregator

// TemporalAggType represents a temporal agg type value.
type TemporalAggType = temporal.TemporalAggType

// AggregationBucket represents a aggregation bucket value.
type AggregationBucket = temporal.AggregationBucket

// TemporalAggRequest represents a temporal agg request value.
type TemporalAggRequest = temporal.TemporalAggRequest

// TemporalResultCache represents a temporal result cache value.
type TemporalResultCache = temporal.TemporalResultCache

// TemporalCacheStats represents a temporal cache stats value.
type TemporalCacheStats = temporal.TemporalCacheStats

// TemporalConfig represents a temporal config value.
type TemporalConfig = temporal.TemporalConfig

// TemporalVector represents a temporal vector value.
type TemporalVector = temporal.TemporalVector

// TemporalIndex represents a temporal index value.
type TemporalIndex = temporal.TemporalIndex

// VectorTimestamp represents a vector timestamp value.
type VectorTimestamp = temporal.VectorTimestamp

// TemporalPredicate represents a temporal predicate value.
type TemporalPredicate = temporal.TemporalPredicate

// SlidingWindowPredicate represents a sliding window predicate value.
type SlidingWindowPredicate = temporal.SlidingWindowPredicate

// TemporalEntry represents a temporal entry value.
type TemporalEntry = temporal.TemporalEntry

// TemporalNode represents a temporal node value.
type TemporalNode = temporal.TemporalNode

// TemporalTree represents a temporal tree value.
type TemporalTree = temporal.TemporalTree

// VersionedVector represents a versioned vector value.
type VersionedVector = temporal.VersionedVector

// VersionHistory represents a version history value.
type VersionHistory = temporal.VersionHistory

// VersionHistoryConfig represents a version history config value.
type VersionHistoryConfig = temporal.VersionHistoryConfig

const (
	// TemporalAggCount is a temporal agg count constant.
	TemporalAggCount = temporal.TemporalAggCount
	// TemporalAggMin is a temporal agg min constant.
	TemporalAggMin = temporal.TemporalAggMin
	// TemporalAggMax is a temporal agg max constant.
	TemporalAggMax = temporal.TemporalAggMax
	// TemporalAggMean is a temporal agg mean constant.
	TemporalAggMean = temporal.TemporalAggMean
	// TemporalAggSum is a temporal agg sum constant.
	TemporalAggSum = temporal.TemporalAggSum
	// TemporalShards is a temporal shards constant.
	TemporalShards = temporal.TemporalShards
)

// DefaultTemporalConfig returns the default TemporalConfig.
func DefaultTemporalConfig() TemporalConfig {
	return temporal.DefaultTemporalConfig()
}

// DefaultVersionHistoryConfig returns the default VersionHistoryConfig.
func DefaultVersionHistoryConfig() VersionHistoryConfig {
	return temporal.DefaultVersionHistoryConfig()
}

// NewSegmentTree constructs a new SegmentTree.
func NewSegmentTree(min, max int64) *SegmentTree {
	return temporal.NewSegmentTree(min, max)
}

// NewTemporalAggregator constructs a new TemporalAggregator.
func NewTemporalAggregator(maxBuckets int) *TemporalAggregator {
	return temporal.NewTemporalAggregator(maxBuckets)
}

// NewTemporalResultCache constructs a new TemporalResultCache.
func NewTemporalResultCache(size int) *TemporalResultCache {
	return temporal.NewTemporalResultCache(size)
}

// NewTemporalIndex constructs a new TemporalIndex.
func NewTemporalIndex(dimension int) *TemporalIndex {
	return temporal.NewTemporalIndex(dimension)
}

// NewTemporalTree constructs a new TemporalTree.
func NewTemporalTree(arena *memory.SlabArena) *TemporalTree {
	return temporal.NewTemporalTree(arena)
}

// NewVersionHistory constructs a new VersionHistory.
func NewVersionHistory(cfg VersionHistoryConfig) *VersionHistory {
	return temporal.NewVersionHistory(cfg)
}
