package store

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/cluster"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

type SearchResultPool = types.SearchResultPool
type PeerReplicator = cluster.PeerReplicator
type InvertedIndex = index.InvertedIndex
type BM25InvertedIndex = index.BM25InvertedIndex
type BM25ArenaIndex = index.BM25ArenaIndex
type ColumnInvertedIndex = index.ColumnInvertedIndex
type RuntimeIndexAdapter = index.RuntimeIndexAdapter
type IndexPerformancePredictor = index.IndexPerformancePredictor
type LearnedIndexRateLimiter = index.LearnedIndexRateLimiter
type FlightClientPool = cluster.FlightClientPool
type ChangeDataCapture = cluster.ChangeDataCapture
type CircuitBreakerRegistry = cluster.CircuitBreakerRegistry
type SplitBrainDetector = cluster.SplitBrainDetector
type FlightClientPoolConfig = cluster.FlightClientPoolConfig
type QuorumManager = cluster.QuorumManager
type ReplicatorConfig = cluster.ReplicatorConfig
type BM25Config = index.BM25Config
type CDCSubscription = cluster.CDCSubscription
type ShardedHNSW = index.ShardedHNSW
type ShardedInvertedIndex = index.ShardedInvertedIndex
type LockFreeSlice[T any] = types.LockFreeSlice[T]
type LockFreeMap[K comparable, V any] = types.LockFreeMap[K, V]
type RowPosition = index.RowPosition

func NewSearchResultPool() *SearchResultPool { return types.NewSearchResultPool() }
func NewPeerReplicator(cfg cluster.ReplicatorConfig) *PeerReplicator {
	return cluster.NewPeerReplicator(cfg)
}
func DefaultReplicatorConfig() cluster.ReplicatorConfig { return cluster.DefaultReplicatorConfig() }
func DefaultFlightClientPoolConfig() FlightClientPoolConfig {
	return cluster.DefaultFlightClientPoolConfig()
}
func NewFlightClientPool(cfg FlightClientPoolConfig) *FlightClientPool {
	return cluster.NewFlightClientPool(cfg)
}
func NewChangeDataCapture(s cluster.CDCStore, l zerolog.Logger) *ChangeDataCapture {
	return cluster.NewChangeDataCapture(s, l)
}
func NewSplitBrainDetector(cfg cluster.SplitBrainConfig) *SplitBrainDetector {
	return cluster.NewSplitBrainDetector(cfg)
}
func NewLockFreeSlice[T any]() *LockFreeSlice[T]   { return types.NewLockFreeSlice[T]() }
func NewColumnInvertedIndex() *ColumnInvertedIndex { return index.NewColumnInvertedIndex() }
func NewBM25InvertedIndex(cfg index.BM25Config) *BM25InvertedIndex {
	return index.NewBM25InvertedIndex(cfg)
}
func DefaultBM25Config() index.BM25Config { return index.DefaultBM25Config() }

type QueryFeatures = index.QueryFeatures
type IndexPrediction = index.IndexPrediction
type IndexType = index.IndexType

func NewBM25ArenaIndex(arena *memory.SlabArena, offset int) *BM25ArenaIndex {
	return index.NewBM25ArenaIndex(arena, offset)
}
func NewShardedHNSW(cfg index.ShardedHNSWConfig, dp types.IndexDataProvider) index.VectorIndex {
	return index.NewShardedHNSW(cfg, dp)
}
func DefaultShardedHNSWConfig() index.ShardedHNSWConfig       { return index.DefaultShardedHNSWConfig() }
func NewLockFreeMap[K comparable, V any]() *LockFreeMap[K, V] { return types.NewLockFreeMap[K, V]() }

const IndexTypeHNSW = index.IndexTypeHNSW
const IndexTypeIVFFlat = index.IndexTypeIVFFlat
const IndexTypeDiskANN = index.IndexTypeDiskANN

type CDCFilter = cluster.CDCFilter
type CDCEventType = cluster.CDCEventType

const CDCEventInsert = cluster.CDCEventInsert
const CDCEventUpdate = cluster.CDCEventUpdate
const CDCEventDelete = cluster.CDCEventDelete

type CDCEvent = cluster.CDCEvent
type LearnedIndexConfig = index.LearnedIndexConfig

func NewCircuitBreakerRegistry(cfg cluster.CircuitBreakerConfig) *CircuitBreakerRegistry {
	return cluster.NewCircuitBreakerRegistry(cfg)
}
func DefaultCircuitBreakerConfig() cluster.CircuitBreakerConfig {
	return cluster.DefaultCircuitBreakerConfig()
}

type IndexAdaptationConfig = index.IndexAdaptationConfig

func NewIndexPerformancePredictor(l zerolog.Logger, cfg index.LearnedIndexConfig) *IndexPerformancePredictor {
	return index.NewIndexPerformancePredictor(l, cfg)
}
func NewLearnedIndexRateLimiter(p *IndexPerformancePredictor, l zerolog.Logger) *LearnedIndexRateLimiter {
	return index.NewLearnedIndexRateLimiter(p, l)
}
func NewRuntimeIndexAdapter(l zerolog.Logger, p *IndexPerformancePredictor, cfg IndexAdaptationConfig, m index.MetricsCollector) *RuntimeIndexAdapter {
	return index.NewRuntimeIndexAdapter(l, p, cfg, m)
}

type TrainingSample = index.TrainingSample
type IndexConfig = index.IndexConfig

func NewIndexFactory() *index.IndexFactory { return index.NewIndexFactory() }
func NewPluggableInternalAdapter(i index.PluggableVectorIndex, dp types.IndexDataProvider) index.VectorIndex {
	return index.NewPluggableInternalAdapter(i, dp)
}
func NewInvertedIndex() *InvertedIndex { return index.NewInvertedIndex() }

type ConsistencyLevel = cluster.ConsistencyLevel

func ParseConsistencyLevel(s string) (ConsistencyLevel, error) {
	return cluster.ParseConsistencyLevel(s)
}
func NewLockFreeSliceFrom[T any](items []T) *LockFreeSlice[T] {
	return types.NewLockFreeSliceFrom(items)
}

type IVFHNSWConfig = index.IVFHNSWConfig

func NewIVFHNSWCompositeIndex(d int, cfg IVFHNSWConfig) (*index.IVFHNSWCompositeIndex, error) {
	return index.NewIVFHNSWCompositeIndex(d, cfg)
}

type IVFOPQConfig = index.IVFOPQConfig

func NewIVFOPQIndex(d int, cfg IVFOPQConfig) (*index.IVFOPQIndex, error) {
	return index.NewIVFOPQIndex(d, cfg)
}

type LearnedIndexWithOllama = index.LearnedIndexWithOllama
type OllamaConfig = index.OllamaConfig

func NewLearnedIndexWithOllama(l zerolog.Logger, p *IndexPerformancePredictor, cfg OllamaConfig) *LearnedIndexWithOllama {
	return index.NewLearnedIndexWithOllama(l, p, cfg)
}

type DataServer = cluster.DataServer
type MetaServer = cluster.MetaServer

func NewDataServer(store cluster.FlightBackend) *DataServer { return cluster.NewDataServer(store) }
func NewMetaServer(store cluster.FlightBackend) *MetaServer { return cluster.NewMetaServer(store) }
