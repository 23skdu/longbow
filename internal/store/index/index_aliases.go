package index

import (
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
)

type DistanceMetric = core.DistanceMetric
type VectorDataType = types.VectorDataType
type VectorID = types.VectorID
type Location = types.Location
type SearchResult = types.SearchResult
type Candidate = types.Candidate
type DeltaSync = types.DeltaSync
type ParallelSearchConfig = types.ParallelSearchConfig
type ArrowHNSWConfig = types.ArrowHNSWConfig
type SearchResultPool = types.SearchResultPool
type BitVector = types.BitVector
type RowLocation = types.RowLocation
type VectorIndex = types.VectorIndexer
type IndexJobQueueConfig = types.IndexJobQueueConfig
type IndexJob = types.IndexJob
type IndexJobQueueStats = types.IndexJobQueueStats

// Some global functions or variables that were used from types
var DefaultArrowHNSWConfig = types.DefaultArrowHNSWConfig

type BatchRemapInfo = types.BatchRemapInfo
