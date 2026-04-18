package store

import (
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/storage"
	hnswcore "github.com/23skdu/longbow/internal/store/internal/core"
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

// Index interface alias
type VectorIndex = types.VectorIndexer
type GraphData = types.GraphData
type Candidate = types.Candidate

const (
	MaxNeighbors   = types.MaxNeighbors
	ChunkSize      = types.ChunkSize
	ArrowMaxLayers = types.ArrowMaxLayers
)

// HNSW Core Types
type ArrowHNSW = hnswcore.ArrowHNSW
type ArrowHNSWConfig = types.ArrowHNSWConfig
type ArrowBitset = types.ArrowBitset
type LockFreeRingBuffer[T any] = hnswcore.LockFreeRingBuffer[T]
type PackedAdjacency = hnswcore.PackedAdjacency
type SearchArena = hnswcore.SearchArena
type ChunkedLocationStore = hnswcore.ChunkedLocationStore
type BQEncoder = types.BQEncoder
type RepairAgentConfig = hnswcore.RepairAgentConfig
type SparseLayerIndex = hnswcore.SparseLayerIndex
type IndexJob = types.IndexJob
type IndexJobQueueConfig = types.IndexJobQueueConfig
type RowLocation = types.RowLocation
type IndexJobQueueLockFree = hnswcore.IndexJobQueueLockFree
type IndexJobQueueStats = types.IndexJobQueueStats
type SQ8Encoder = hnswcore.SQ8Encoder
type ArrowSearchContext = hnswcore.ArrowSearchContext
type ArrowSearchContextPool = hnswcore.ArrowSearchContextPool

const (
	DiskGraphMagic   = hnswcore.DiskGraphMagic
	DiskGraphVersion = hnswcore.DiskGraphVersion
)

// Persistence Aliases
type StorageConfig = storage.StorageConfig

// Errors (Aliased from internal/core)

// ErrNotFound indicates a requested resource does not exist.
type ErrNotFound = core.ErrNotFound

// NewNotFoundError creates a not found error.
var NewNotFoundError = core.NewNotFoundError

// Factory functions
var NewArrowHNSW = hnswcore.NewArrowHNSW
var NewArrowHNSWWithConfig = hnswcore.NewArrowHNSWWithConfig
var DefaultArrowHNSWConfig = types.DefaultArrowHNSWConfig
var NewPackedAdjacency = hnswcore.NewPackedAdjacency
var NewArrowBitset = types.NewArrowBitset
var NewChunkedLocationStore = hnswcore.NewChunkedLocationStore
var DefaultIndexJobQueueConfig = types.DefaultIndexJobQueueConfig
var NewIndexJobQueueLockFree = hnswcore.NewIndexJobQueueLockFree
var NewLevelGenerator = hnswcore.NewLevelGenerator
var ExtractVectorFromArrow = hnswcore.ExtractVectorFromArrow
var InferVectorDataType = hnswcore.InferVectorDataType
var GetArena = hnswcore.GetArena
var PutArena = hnswcore.PutArena
var GenerateTestVectors = hnswcore.GenerateTestVectors
var MakeBatchTestRecord = hnswcore.MakeBatchTestRecord
var NewTestHNSWIndex = hnswcore.NewTestHNSWIndex
var NewArrowSearchContext = hnswcore.NewArrowSearchContext
var NewArrowSearchContextPool = hnswcore.NewArrowSearchContextPool
var ExtractVectorAny = hnswcore.ExtractVectorAny
var ExtractVectorF16FromArrow = hnswcore.ExtractVectorF16FromArrow
func ExtractVectorGeneric[T any](rec arrow.RecordBatch, rowIdx, colIdx int) ([]T, error) {
	return hnswcore.ExtractVectorGeneric[T](rec, rowIdx, colIdx)
}
var LookupNeighbors = hnswcore.LookupNeighbors
var ErrVectorNotFound = hnswcore.ErrVectorNotFound
var ErrGetNeighborsNotSupported = hnswcore.ErrGetNeighborsNotSupported
