package store

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/temporal"
)

type SegmentNode = temporal.SegmentNode
type SegmentTree = temporal.SegmentTree
type TemporalAggregator = temporal.TemporalAggregator
type TemporalAggType = temporal.TemporalAggType
type AggregationBucket = temporal.AggregationBucket
type TemporalAggRequest = temporal.TemporalAggRequest
type TemporalResultCache = temporal.TemporalResultCache
type TemporalCacheStats = temporal.TemporalCacheStats
type TemporalConfig = temporal.TemporalConfig
type TemporalVector = temporal.TemporalVector
type TemporalIndex = temporal.TemporalIndex
type VectorTimestamp = temporal.VectorTimestamp
type TemporalPredicate = temporal.TemporalPredicate
type SlidingWindowPredicate = temporal.SlidingWindowPredicate
type TemporalEntry = temporal.TemporalEntry
type TemporalNode = temporal.TemporalNode
type TemporalTree = temporal.TemporalTree
type VersionedVector = temporal.VersionedVector
type VersionHistory = temporal.VersionHistory
type VersionHistoryConfig = temporal.VersionHistoryConfig

const (
	TemporalAggCount = temporal.TemporalAggCount
	TemporalAggMin   = temporal.TemporalAggMin
	TemporalAggMax   = temporal.TemporalAggMax
	TemporalAggMean  = temporal.TemporalAggMean
	TemporalAggSum   = temporal.TemporalAggSum
	TemporalShards   = temporal.TemporalShards
)

func DefaultTemporalConfig() TemporalConfig {
	return temporal.DefaultTemporalConfig()
}

func DefaultVersionHistoryConfig() VersionHistoryConfig {
	return temporal.DefaultVersionHistoryConfig()
}

func NewSegmentTree(min, max int64) *SegmentTree {
	return temporal.NewSegmentTree(min, max)
}

func NewTemporalAggregator(maxBuckets int) *TemporalAggregator {
	return temporal.NewTemporalAggregator(maxBuckets)
}

func NewTemporalResultCache(size int) *TemporalResultCache {
	return temporal.NewTemporalResultCache(size)
}

func NewTemporalIndex(dimension int) *TemporalIndex {
	return temporal.NewTemporalIndex(dimension)
}

func NewTemporalTree(arena *memory.SlabArena) *TemporalTree {
	return temporal.NewTemporalTree(arena)
}

func NewVersionHistory(cfg VersionHistoryConfig) *VersionHistory {
	return temporal.NewVersionHistory(cfg)
}
