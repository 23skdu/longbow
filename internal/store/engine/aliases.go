package engine

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/cluster"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

// SearchResultPool represents a search result pool value.
type SearchResultPool = types.SearchResultPool

// PeerReplicator represents a peer replicator value.
type PeerReplicator = cluster.PeerReplicator

// InvertedIndex represents a inverted index value.
type InvertedIndex = index.InvertedIndex

// BM25InvertedIndex represents a bm25 inverted index value.
type BM25InvertedIndex = index.BM25InvertedIndex

// BM25ArenaIndex represents a bm25 arena index value.
type BM25ArenaIndex = index.BM25ArenaIndex

// BM25Document represents a bm25 document value.
type BM25Document = index.BM25Document

// BM25ArenaDocument represents a bm25 arena document value.
type BM25ArenaDocument = index.BM25ArenaDocument

// ColumnInvertedIndex represents a column inverted index value.
type ColumnInvertedIndex = index.ColumnInvertedIndex

// RuntimeIndexAdapter represents a runtime index adapter value.
type RuntimeIndexAdapter = index.RuntimeIndexAdapter

// IndexPerformancePredictor represents a index performance predictor value.
type IndexPerformancePredictor = index.IndexPerformancePredictor

// LearnedIndexRateLimiter represents a learned index rate limiter value.
type LearnedIndexRateLimiter = index.LearnedIndexRateLimiter

// FlightClientPool represents a flight client pool value.
type FlightClientPool = cluster.FlightClientPool

// ChangeDataCapture represents a change data capture value.
type ChangeDataCapture = cluster.ChangeDataCapture

// CircuitBreakerRegistry represents a circuit breaker registry value.
type CircuitBreakerRegistry = cluster.CircuitBreakerRegistry

// SplitBrainDetector represents a split brain detector value.
type SplitBrainDetector = cluster.SplitBrainDetector

// FlightClientPoolConfig represents a flight client pool config value.
type FlightClientPoolConfig = cluster.FlightClientPoolConfig

// QuorumManager represents a quorum manager value.
type QuorumManager = cluster.QuorumManager

// ReplicatorConfig represents a replicator config value.
type ReplicatorConfig = cluster.ReplicatorConfig

// BM25Config represents a bm25 config value.
type BM25Config = index.BM25Config

// CDCSubscription represents a cdc subscription value.
type CDCSubscription = cluster.CDCSubscription

// ShardedHNSW represents a sharded hnsw value.
type ShardedHNSW = index.ShardedHNSW

// ShardedInvertedIndex represents a sharded inverted index value.
type ShardedInvertedIndex = index.ShardedInvertedIndex

// LockFreeSlice represents a lock free slice value.
type LockFreeSlice[T any] = types.LockFreeSlice[T]

// LockFreeMap represents a lock free map value.
type LockFreeMap[K comparable, V any] = types.LockFreeMap[K, V]

// RowPosition represents a row position value.
type RowPosition = index.RowPosition

// NewSearchResultPool constructs a new SearchResultPool.
func NewSearchResultPool() *SearchResultPool { return types.NewSearchResultPool() }

// NewPeerReplicator constructs a new PeerReplicator.
func NewPeerReplicator(cfg cluster.ReplicatorConfig) *PeerReplicator {
	return cluster.NewPeerReplicator(cfg)
}

// DefaultReplicatorConfig returns the default ReplicatorConfig.
func DefaultReplicatorConfig() cluster.ReplicatorConfig { return cluster.DefaultReplicatorConfig() }

// DefaultFlightClientPoolConfig returns the default FlightClientPoolConfig.
func DefaultFlightClientPoolConfig() FlightClientPoolConfig {
	return cluster.DefaultFlightClientPoolConfig()
}

// NewFlightClientPool constructs a new FlightClientPool.
func NewFlightClientPool(cfg FlightClientPoolConfig) *FlightClientPool {
	return cluster.NewFlightClientPool(cfg)
}

// NewChangeDataCapture constructs a new ChangeDataCapture.
func NewChangeDataCapture(s cluster.CDCStore, l zerolog.Logger) *ChangeDataCapture {
	return cluster.NewChangeDataCapture(s, l)
}

// NewSplitBrainDetector constructs a new SplitBrainDetector.
func NewSplitBrainDetector(cfg cluster.SplitBrainConfig) *SplitBrainDetector {
	return cluster.NewSplitBrainDetector(cfg)
}

// NewLockFreeSlice constructs a new LockFreeSlice.
func NewLockFreeSlice[T any]() *LockFreeSlice[T] { return types.NewLockFreeSlice[T]() }

// NewColumnInvertedIndex constructs a new ColumnInvertedIndex.
func NewColumnInvertedIndex() *ColumnInvertedIndex { return index.NewColumnInvertedIndex() }

// NewBM25InvertedIndex constructs a new BM25InvertedIndex.
func NewBM25InvertedIndex(cfg index.BM25Config) *BM25InvertedIndex {
	return index.NewBM25InvertedIndex(cfg)
}

// DefaultBM25Config returns the default BM25Config.
func DefaultBM25Config() index.BM25Config { return index.DefaultBM25Config() }

// QueryFeatures represents a query features value.
type QueryFeatures = index.QueryFeatures

// IndexPrediction represents a index prediction value.
type IndexPrediction = index.IndexPrediction

// IndexType represents a index type value.
type IndexType = index.IndexType

// NewBM25ArenaIndex constructs a new BM25ArenaIndex.
func NewBM25ArenaIndex(arena *memory.SlabArena, offset int) *BM25ArenaIndex {
	return index.NewBM25ArenaIndex(arena, offset)
}

// NewShardedHNSW constructs a new ShardedHNSW.
func NewShardedHNSW(cfg index.ShardedHNSWConfig, dp types.IndexDataProvider) index.VectorIndex {
	return index.NewShardedHNSW(cfg, dp)
}

// DefaultShardedHNSWConfig returns the default ShardedHNSWConfig.
func DefaultShardedHNSWConfig() index.ShardedHNSWConfig { return index.DefaultShardedHNSWConfig() }

// NewLockFreeMap constructs a new LockFreeMap.
func NewLockFreeMap[K comparable, V any]() *LockFreeMap[K, V] { return types.NewLockFreeMap[K, V]() }

// IndexTypeHNSW is a index type hnsw constant.
const IndexTypeHNSW = index.IndexTypeHNSW

// IndexTypeIVFFlat is a index type ivf flat constant.
const IndexTypeIVFFlat = index.IndexTypeIVFFlat

// IndexTypeDiskANN is a index type disk ann constant.
const IndexTypeDiskANN = index.IndexTypeDiskANN

// CDCFilter represents a cdc filter value.
type CDCFilter = cluster.CDCFilter

// CDCEventType represents a cdc event type value.
type CDCEventType = cluster.CDCEventType

// CDCEventInsert is a cdc event insert constant.
const CDCEventInsert = cluster.CDCEventInsert

// CDCEventUpdate is a cdc event update constant.
const CDCEventUpdate = cluster.CDCEventUpdate

// CDCEventDelete is a cdc event delete constant.
const CDCEventDelete = cluster.CDCEventDelete

// CDCEvent represents a cdc event value.
type CDCEvent = cluster.CDCEvent

// LearnedIndexConfig represents a learned index config value.
type LearnedIndexConfig = index.LearnedIndexConfig

// NewCircuitBreakerRegistry constructs a new CircuitBreakerRegistry.
func NewCircuitBreakerRegistry(cfg cluster.CircuitBreakerConfig) *CircuitBreakerRegistry {
	return cluster.NewCircuitBreakerRegistry(cfg)
}

// DefaultCircuitBreakerConfig returns the default CircuitBreakerConfig.
func DefaultCircuitBreakerConfig() cluster.CircuitBreakerConfig {
	return cluster.DefaultCircuitBreakerConfig()
}

// IndexAdaptationConfig represents a index adaptation config value.
type IndexAdaptationConfig = index.IndexAdaptationConfig

// NewIndexPerformancePredictor constructs a new IndexPerformancePredictor.
func NewIndexPerformancePredictor(l zerolog.Logger, cfg index.LearnedIndexConfig) *IndexPerformancePredictor {
	return index.NewIndexPerformancePredictor(l, cfg)
}

// NewLearnedIndexRateLimiter constructs a new LearnedIndexRateLimiter.
func NewLearnedIndexRateLimiter(p *IndexPerformancePredictor, l zerolog.Logger) *LearnedIndexRateLimiter {
	return index.NewLearnedIndexRateLimiter(p, l)
}

// NewRuntimeIndexAdapter constructs a new RuntimeIndexAdapter.
func NewRuntimeIndexAdapter(l zerolog.Logger, p *IndexPerformancePredictor, cfg IndexAdaptationConfig, m index.MetricsCollector) *RuntimeIndexAdapter {
	return index.NewRuntimeIndexAdapter(l, p, cfg, m)
}

// TrainingSample represents a training sample value.
type TrainingSample = index.TrainingSample

// IndexConfig represents a index config value.
type IndexConfig = index.IndexConfig

// NewIndexFactory constructs a new IndexFactory.
func NewIndexFactory() *index.IndexFactory { return index.NewIndexFactory() }

// NewPluggableInternalAdapter constructs a new PluggableInternalAdapter.
func NewPluggableInternalAdapter(i index.PluggableVectorIndex, dp types.IndexDataProvider) index.VectorIndex {
	return index.NewPluggableInternalAdapter(i, dp)
}

// NewInvertedIndex constructs a new InvertedIndex.
func NewInvertedIndex() *InvertedIndex { return index.NewInvertedIndex() }

// ConsistencyLevel represents a consistency level value.
type ConsistencyLevel = cluster.ConsistencyLevel

// ParseConsistencyLevel runs the parse consistency level operation.
func ParseConsistencyLevel(s string) (ConsistencyLevel, error) {
	return cluster.ParseConsistencyLevel(s)
}

// NewLockFreeSliceFrom constructs a new LockFreeSliceFrom.
func NewLockFreeSliceFrom[T any](items []T) *LockFreeSlice[T] {
	return types.NewLockFreeSliceFrom(items)
}

// IVFHNSWConfig represents a ivfhnsw config value.
type IVFHNSWConfig = index.IVFHNSWConfig

// NewIVFHNSWCompositeIndex constructs a new IVFHNSWCompositeIndex.
func NewIVFHNSWCompositeIndex(d int, cfg IVFHNSWConfig) (*index.IVFHNSWCompositeIndex, error) {
	return index.NewIVFHNSWCompositeIndex(d, cfg)
}

// IVFOPQConfig represents a ivfopq config value.
type IVFOPQConfig = index.IVFOPQConfig

// NewIVFOPQIndex constructs a new IVFOPQIndex.
func NewIVFOPQIndex(d int, cfg IVFOPQConfig) (*index.IVFOPQIndex, error) {
	return index.NewIVFOPQIndex(d, cfg)
}

// LearnedIndexWithOllama represents a learned index with ollama value.
type LearnedIndexWithOllama = index.LearnedIndexWithOllama

// OllamaConfig represents a ollama config value.
type OllamaConfig = index.OllamaConfig

// NewLearnedIndexWithOllama constructs a new LearnedIndexWithOllama.
func NewLearnedIndexWithOllama(l zerolog.Logger, p *IndexPerformancePredictor, cfg OllamaConfig) *LearnedIndexWithOllama {
	return index.NewLearnedIndexWithOllama(l, p, cfg)
}

// DataServer represents a data server value.
type DataServer = cluster.DataServer

// MetaServer represents a meta server value.
type MetaServer = cluster.MetaServer

// NewDataServer constructs a new DataServer.
func NewDataServer(store cluster.FlightBackend) *DataServer { return cluster.NewDataServer(store) }

// NewMetaServer constructs a new MetaServer.
func NewMetaServer(store cluster.FlightBackend) *MetaServer { return cluster.NewMetaServer(store) }
