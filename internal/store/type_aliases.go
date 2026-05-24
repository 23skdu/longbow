package store

import (
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/storage"
	hnswcore "github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

// Type Aliases to maintain backward compatibility while migrating to internal/core

// VectorID is a unique identifier for a vector in the system.
type VectorID = core.VectorID

// Location maps a VectorID to a physical location in a Dataset (Batch + Row).
type Location = types.Location

// DistanceMetric defines the distance metric used for vector comparison.
type DistanceMetric = core.DistanceMetric

const (
	// MetricEuclidean is the default L2 distance (lower is closer).
	MetricEuclidean = core.MetricEuclidean
	// MetricCosine is the Cosine distance (1.0 - cosine_similarity).
	MetricCosine = core.MetricCosine
	// MetricDotProduct is the Inner Product (higher is usually better).
	MetricDotProduct = core.MetricDotProduct
)

// VectorIndex is an interface for vector indexing operations.
type VectorIndex = types.VectorIndexer

// GraphData represents the graph structure for HNSW.
type GraphData = types.GraphData

// Candidate represents a potential search result during HNSW traversal.
type Candidate = types.Candidate

const (
	// MaxNeighbors is the maximum number of neighbors allowed for a node in the HNSW graph.
	MaxNeighbors = types.MaxNeighbors
	// ChunkSize is the number of elements processed in a single chunk.
	ChunkSize = types.ChunkSize
	// ArrowMaxLayers is the maximum number of layers in the Arrow HNSW index.
	ArrowMaxLayers = types.ArrowMaxLayers
)

// DeltaSync represents a delta update for index synchronization.
type DeltaSync = types.DeltaSync

// ParallelSearchConfig defines configuration for parallel search operations.
type ParallelSearchConfig = types.ParallelSearchConfig

// ArrowHNSW is the core HNSW implementation using Arrow memory.
type ArrowHNSW = hnswcore.ArrowHNSW

// ArrowHNSWConfig defines the configuration for ArrowHNSW.
type ArrowHNSWConfig = types.ArrowHNSWConfig

// ArrowBitset provides a fast bitset implementation for visited nodes.
type ArrowBitset = types.ArrowBitset

// LockFreeRingBuffer is a high-performance concurrent ring buffer.
type LockFreeRingBuffer[T any] = hnswcore.LockFreeRingBuffer[T]

// PackedAdjacency stores graph neighbors in a compact, cache-friendly format.
type PackedAdjacency = hnswcore.PackedAdjacency

// SearchArena provides pre-allocated memory for search operations to reduce heap churn.
type SearchArena = hnswcore.SearchArena

// ChunkedLocationStore manages vector location mappings in memory-efficient chunks.
type ChunkedLocationStore = hnswcore.ChunkedLocationStore

// BQEncoder implements Binary Quantization.
type BQEncoder = types.BQEncoder

// RepairAgentConfig defines the configuration for the HNSW repair agent.
type RepairAgentConfig = hnswcore.RepairAgentConfig

// SparseLayerIndex manages sparse index layers.
type SparseLayerIndex = hnswcore.SparseLayerIndex

// IndexJob represents a background indexing task.
type IndexJob = types.IndexJob

// IndexJobQueueConfig defines configuration for the index job queue.
type IndexJobQueueConfig = types.IndexJobQueueConfig

// RowLocation maps a row to its physical storage.
type RowLocation = types.RowLocation

// IndexJobQueueLockFree is a high-performance index job queue.
type IndexJobQueueLockFree = hnswcore.IndexJobQueueLockFree

// IndexJobQueueStats provides metrics for the index job queue.
type IndexJobQueueStats = types.IndexJobQueueStats

// SQ8Encoder implements 8-bit Scalar Quantization.
type SQ8Encoder = hnswcore.SQ8Encoder

// ArrowSearchContext holds state for a single HNSW search.
type ArrowSearchContext = hnswcore.ArrowSearchContext

// ArrowSearchContextPool provides a pool for reusing search contexts.
type ArrowSearchContextPool = hnswcore.ArrowSearchContextPool

const (
	// DiskGraphMagic is a magic number used to identify DiskGraph files.
	DiskGraphMagic = hnswcore.DiskGraphMagic
	// DiskGraphVersion is the current version of the DiskGraph file format.
	DiskGraphVersion = hnswcore.DiskGraphVersion
)

// StorageConfig defines the configuration for the persistence layer.
type StorageConfig = storage.StorageConfig

// Errors (Aliased from internal/core)

// ErrNotFound indicates a requested resource does not exist.
type ErrNotFound = core.ErrNotFound

// NewNotFoundError creates a not found error.
var NewNotFoundError = core.NewNotFoundError

// NewArrowHNSW creates a new instance of the ArrowHNSW index.
var NewArrowHNSW = hnswcore.NewArrowHNSW

// NewArrowHNSWWithConfig creates a new HNSW index instance with a custom configuration.
var NewArrowHNSWWithConfig = hnswcore.NewArrowHNSWWithConfig

// DefaultArrowHNSWConfig returns the default configuration for HNSW indices.
var DefaultArrowHNSWConfig = types.DefaultArrowHNSWConfig

// NewPackedAdjacency creates a new packed adjacency list for graph storage.
var NewPackedAdjacency = hnswcore.NewPackedAdjacency

// NewArrowBitset creates a new bitset for tracking visited nodes during search.
var NewArrowBitset = types.NewArrowBitset

// NewChunkedLocationStore creates a new store for tracking vector locations.
var NewChunkedLocationStore = hnswcore.NewChunkedLocationStore

// DefaultIndexJobQueueConfig returns the default configuration for the background indexing queue.
var DefaultIndexJobQueueConfig = types.DefaultIndexJobQueueConfig

// NewIndexJobQueueLockFree creates a new lock-free queue for background indexing jobs.
var NewIndexJobQueueLockFree = hnswcore.NewIndexJobQueueLockFree

// NewLevelGenerator creates a new generator for HNSW node levels.
var NewLevelGenerator = hnswcore.NewLevelGenerator

// ExtractVectorFromArrow extracts a vector from an Arrow record batch.
var ExtractVectorFromArrow = hnswcore.ExtractVectorFromArrow

// InferVectorDataType infers the vector data type from an Arrow field.
var InferVectorDataType = hnswcore.InferVectorDataType

// GetArena returns a search arena from the global pool.
var GetArena = hnswcore.GetArena

// GetArenaForNode returns a search arena from the global pool for a specific NUMA node.
var GetArenaForNode = hnswcore.GetArenaForNode

// PutArena returns a search arena to the global pool.
var PutArena = hnswcore.PutArena

// GenerateTestVectors generates a set of random vectors for testing.
var GenerateTestVectors = hnswcore.GenerateTestVectors

// MakeBatchTestRecord creates an Arrow record batch for testing.
var MakeBatchTestRecord = hnswcore.MakeBatchTestRecord

// NewTestHNSWIndex creates a small HNSW index for unit testing.
var NewTestHNSWIndex = hnswcore.NewTestHNSWIndex

// NewArrowSearchContext creates a new search context.
var NewArrowSearchContext = hnswcore.NewArrowSearchContext

// NewArrowSearchContextPool creates a new pool for search contexts.
var NewArrowSearchContextPool = hnswcore.NewArrowSearchContextPool

// ExtractVectorAny extracts a vector as an interface{} from an Arrow record batch.
var ExtractVectorAny = hnswcore.ExtractVectorAny

// ExtractVectorF16FromArrow extracts a float16 vector from an Arrow record batch.
var ExtractVectorF16FromArrow = hnswcore.ExtractVectorF16FromArrow

// ExtractVectorGeneric extracts a vector of a generic type from an Arrow record batch.
func ExtractVectorGeneric[T any](rec arrow.RecordBatch, rowIdx, colIdx int) ([]T, error) {
	return hnswcore.ExtractVectorGeneric[T](rec, rowIdx, colIdx)
}

// LookupNeighbors retrieves the neighbors of a node from the graph.
var LookupNeighbors = hnswcore.LookupNeighbors

// ErrVectorNotFound is returned when a requested vector is not found in the index.
var ErrVectorNotFound = hnswcore.ErrVectorNotFound

// ErrGetNeighborsNotSupported is returned when the index doesn't support neighbor retrieval.
var ErrGetNeighborsNotSupported = hnswcore.ErrGetNeighborsNotSupported

// TemporalSearchRequest defines the parameters for a temporal search operation.
type TemporalSearchRequest = core.TemporalSearchRequest

// TemporalAggregationRequest defines a request for aggregating data over a time window.
type TemporalAggregationRequest = core.TemporalAggregationRequest

// TemporalVersionHistoryRequest defines a request for retrieving the version history of a vector.
type TemporalVersionHistoryRequest = core.TemporalVersionHistoryRequest

// LockFreeNeighborList provides lock-free reads with copy-on-write updates.
type LockFreeNeighborList = hnswcore.LockFreeNeighborList

// NewLockFreeNeighborList creates a new lock-free neighbor list.
var NewLockFreeNeighborList = hnswcore.NewLockFreeNeighborList

// LockFreeNeighborCache provides a cache of lock-free neighbor lists.
type LockFreeNeighborCache = hnswcore.LockFreeNeighborCache

// NewLockFreeNeighborCache creates a new neighbor cache.
var NewLockFreeNeighborCache = hnswcore.NewLockFreeNeighborCache
