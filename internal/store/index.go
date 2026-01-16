package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

// VectorIndex defines the interface for vector index implementations.
// This allows for both single-threaded and sharded index implementations.

// VectorDataType represents the underlying numerical type of vector elements.
type VectorDataType int

const (
	VectorTypeUnknown VectorDataType = iota
	VectorTypeInt8
	VectorTypeUint8
	VectorTypeInt16
	VectorTypeUint16
	VectorTypeInt32
	VectorTypeUint32
	VectorTypeInt64
	VectorTypeUint64
	VectorTypeFloat16
	VectorTypeFloat32
	VectorTypeFloat64
	VectorTypeComplex64
	VectorTypeComplex128
)

func (t VectorDataType) String() string {
	switch t {
	case VectorTypeUnknown:
		return "unknown"
	case VectorTypeInt8:
		return "int8"
	case VectorTypeUint8:
		return "uint8"
	case VectorTypeInt16:
		return "int16"
	case VectorTypeUint16:
		return "uint16"
	case VectorTypeInt32:
		return "int32"
	case VectorTypeUint32:
		return "uint32"
	case VectorTypeInt64:
		return "int64"
	case VectorTypeUint64:
		return "uint64"
	case VectorTypeFloat16:
		return "float16"
	case VectorTypeFloat32:
		return "float32"
	case VectorTypeFloat64:
		return "float64"
	case VectorTypeComplex64:
		return "complex64"
	case VectorTypeComplex128:
		return "complex128"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// ElementSize returns the size of a single element of this type in bytes.
func (t VectorDataType) ElementSize() int {
	switch t {
	case VectorTypeUnknown:
		return 0
	case VectorTypeInt8, VectorTypeUint8:
		return 1
	case VectorTypeInt16, VectorTypeUint16, VectorTypeFloat16:
		return 2
	case VectorTypeInt32, VectorTypeUint32, VectorTypeFloat32:
		return 4
	case VectorTypeInt64, VectorTypeUint64, VectorTypeFloat64, VectorTypeComplex64:
		return 8
	case VectorTypeComplex128:
		return 16
	default:
		return 0
	}
}

type SearchResult struct {
	ID     VectorID
	Score  float32
	Vector []byte
	Type   VectorDataType
}

type SearchOptions struct {
	IncludeVectors bool
	VectorFormat   string         // "quantized", "raw"
	TargetType     VectorDataType // Desired type if conversion is requested
}

type VectorIndex interface {
	// AddByLocation adds a vector from the dataset using batch and row indices.
	// Returns the assigned internal Vector ID.
	AddByLocation(batchIdx, rowIdx int) (uint32, error)

	// AddByRecord adds a vector directly from a record batch.
	// Returns the assigned internal Vector ID.
	AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error)

	// AddBatch adds multiple vectors from multiple record batches efficiently.
	// Returns the assigned internal Vector IDs.
	AddBatch(recs []arrow.RecordBatch, rowIdxs []int, batchIdxs []int) ([]uint32, error)

	// SearchVectors returns the k nearest neighbors for the query vector with scores and optional filtering.
	// query can be []float32, []float16.Num, []complex64, []complex128, etc.
	SearchVectors(query any, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error)

	// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
	SearchVectorsWithBitmap(query any, k int, filter *query.Bitset, options SearchOptions) []SearchResult

	// GetLocation retrieves the storage location for a given vector ID.
	// Returns the location and true if found, or zero location and false if not found.
	GetLocation(id VectorID) (Location, bool)

	// GetVectorID retrieves the vector ID for a given storage location.
	// Returns the ID and true if found, or 0 and false if not found.
	GetVectorID(loc Location) (VectorID, bool)

	// GetNeighbors returns the nearest neighbors for a given vector ID from the graph.
	GetNeighbors(id VectorID) ([]VectorID, error)

	// Len returns the number of vectors in the index.
	Len() int

	// GetDimension returns the vector dimension.
	GetDimension() uint32

	// Warmup pre-loads index data into memory
	Warmup() int

	// SetIndexedColumns updates columns that should be indexed for fast equality lookups
	SetIndexedColumns(cols []string)

	// EstimateMemory returns the estimated memory usage of the index in bytes
	EstimateMemory() int64

	// TrainPQ trains the PQ encoder with provided sample vectors.
	TrainPQ(vectors [][]float32) error
	// GetPQEncoder returns the current PQ encoder, if any.
	GetPQEncoder() *pq.PQEncoder

	// Close releases index resources.
	Close() error
}
