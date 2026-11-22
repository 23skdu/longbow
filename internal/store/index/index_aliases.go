package index

import (
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
)

// DistanceMetric represents a distance metric value.
type DistanceMetric = core.DistanceMetric

// VectorDataType represents a vector data type value.
type VectorDataType = types.VectorDataType

// VectorID represents a vector id value.
type VectorID = types.VectorID

// Location represents a location value.
type Location = types.Location

// SearchResult represents a search result value.
type SearchResult = types.SearchResult

// Candidate represents a candidate value.
type Candidate = types.Candidate

// DeltaSync represents a delta sync value.
type DeltaSync = types.DeltaSync

// ParallelSearchConfig represents a parallel search config value.
type ParallelSearchConfig = types.ParallelSearchConfig

// ArrowHNSWConfig represents a arrow hnsw config value.
type ArrowHNSWConfig = types.ArrowHNSWConfig

// SearchResultPool represents a search result pool value.
type SearchResultPool = types.SearchResultPool

// BitVector represents a bit vector value.
type BitVector = types.BitVector

// RowLocation represents a row location value.
type RowLocation = types.RowLocation

// VectorIndex represents a vector index value.
type VectorIndex = types.VectorIndexer

// IndexJobQueueConfig represents a index job queue config value.
type IndexJobQueueConfig = types.IndexJobQueueConfig

// IndexJob represents a index job value.
type IndexJob = types.IndexJob

// IndexJobQueueStats represents a index job queue stats value.
type IndexJobQueueStats = types.IndexJobQueueStats

// DefaultArrowHNSWConfig global functions or variables that were used from types
var DefaultArrowHNSWConfig = types.DefaultArrowHNSWConfig

// BatchRemapInfo represents a batch remap info value.
type BatchRemapInfo = types.BatchRemapInfo
