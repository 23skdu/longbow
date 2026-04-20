package types

import (
	"fmt"

	basecore "github.com/23skdu/longbow/internal/core"
)

// VectorDataType represents the data type of vector elements
type VectorDataType int

const (
	VectorTypeUnknown VectorDataType = iota
	VectorTypeFloat32
	VectorTypeInt8
	VectorTypeUint8
	VectorTypeFloat16
	VectorTypeFloat64
	VectorTypeComplex64
	VectorTypeComplex128
	VectorTypeInt16
	VectorTypeUint16
	VectorTypeInt32
	VectorTypeUint32
	VectorTypeInt64
	VectorTypeUint64
	VectorTypeBQ
	VectorTypeTQ
)

// String returns a string representation of the vector data type
func (vdt VectorDataType) String() string {
	switch vdt {
	case VectorTypeFloat32:
		return "float32"
	case VectorTypeInt8:
		return "int8"
	case VectorTypeUint8:
		return "uint8"
	case VectorTypeFloat16:
		return "float16"
	case VectorTypeFloat64:
		return "float64"
	case VectorTypeComplex64:
		return "complex64"
	case VectorTypeComplex128:
		return "complex128"
	case VectorTypeInt32:
		return "int32"
	case VectorTypeUint32:
		return "uint32"
	case VectorTypeInt16:
		return "int16"
	case VectorTypeUint16:
		return "uint16"
	case VectorTypeInt64:
		return "int64"
	case VectorTypeUint64:
		return "uint64"
	case VectorTypeTQ:
		return "turboquant"
	default:
		return fmt.Sprintf("unknown(%d)", int(vdt))
	}
}

// MapStringToVectorDataType maps a string representation to its VectorDataType
func MapStringToVectorDataType(s string) VectorDataType {
	switch s {
	case "float32":
		return VectorTypeFloat32
	case "float16":
		return VectorTypeFloat16
	case "int8":
		return VectorTypeInt8
	case "uint8":
		return VectorTypeUint8
	case "float64":
		return VectorTypeFloat64
	case "complex64":
		return VectorTypeComplex64
	case "complex128":
		return VectorTypeComplex128
	case "int32":
		return VectorTypeInt32
	case "uint32":
		return VectorTypeUint32
	case "bq":
		return VectorTypeBQ
	case "turboquant", "tq":
		return VectorTypeTQ
	default:
		return VectorTypeUnknown
	}
}

// ElementSize returns the size in bytes of one element of this data type
func (vdt VectorDataType) ElementSize() int {
	switch vdt {
	case VectorTypeInt8, VectorTypeUint8:
		return 1
	case VectorTypeFloat16, VectorTypeInt16, VectorTypeUint16:
		return 2
	case VectorTypeFloat32, VectorTypeInt32, VectorTypeUint32:
		return 4
	case VectorTypeFloat64, VectorTypeInt64, VectorTypeUint64, VectorTypeComplex64:
		return 8
	case VectorTypeComplex128:
		return 16
	default:
		return 0
	}
}

// Candidate represents a search result candidate with ID and distance
type Candidate = basecore.Candidate

// MaxNeighbors is the maximum number of neighbors per node in HNSW
const MaxNeighbors = 128

// ChunkSize is the size of data chunks for memory allocation
const ChunkSize = 1024

// ArrowMaxLayers is the maximum number of layers in HNSW hierarchy
const ArrowMaxLayers = 16

// NodeLockMask is used for atomic per-node locking in the Version field.
// HNSW version fields are uint32: [Lock Bit (1 bit) | Version (31 bits)]
const NodeLockMask uint32 = 1 << 31

// VectorID is a type alias for vector identifiers
type VectorID = basecore.VectorID

// SearchResult represents a single flight search result
type SearchResult = basecore.SearchResult

// Location represents a physical location of a row
type Location = basecore.Location

// LocationChunkSize is the size of chunks in the location store
const LocationChunkSize = 1024

// SearchOptions defines the options for search operations
type SearchOptions struct {
	IncludeVectors bool
	VectorFormat   VectorDataType
	Filter         any        // Legacy filter mechanism
	FilterExpr     FilterExpr // Rich AST JSON Filter
	ExactK         bool
	Ef             int            // Entry factor for HNSW search (controls search breadth)
	Consistency    string         // "eventual" | "strong" | "" (default = eventual)
	VectorType     VectorDataType // Explicit index type selection for search (float32, turboquant)
	TurboQuantBits int            // Bit depth for TurboQuant search (4, 8)
}

// DefaultSearchOptions returns SearchOptions with production defaults.
func DefaultSearchOptions() SearchOptions {
	return SearchOptions{
		Ef:          50,
		Consistency: "eventual",
	}
}

// FilterExpr is the AST node interface for evaluating complex boolean nesting conditions.
type FilterExpr interface {
	Evaluate(metadata map[string]interface{}) bool
}

// ChunkID returns the chunk index for a given vector identifier
func ChunkID(id uint32) int {
	return int(id / uint32(ChunkSize))
}

// ChunkOffset returns the offset within a chunk for a given vector identifier
func ChunkOffset(id uint32) int {
	return int(id % uint32(ChunkSize))
}

// ErrVectorDimensionMismatch represents a mismatch between vector dimensions.
type ErrVectorDimensionMismatch struct {
	ID       int
	Expected int
	Actual   int
}

func (e *ErrVectorDimensionMismatch) Error() string {
	return fmt.Sprintf("dimension mismatch for vector %d: expected %d, got %d", e.ID, e.Expected, e.Actual)
}

// NewVectorDimensionMismatchError creates a new ErrVectorDimensionMismatch.
func NewVectorDimensionMismatchError(id, expected, actual int) error {
	return &ErrVectorDimensionMismatch{
		ID:       id,
		Expected: expected,
		Actual:   actual,
	}
}

// ErrNeighborSelectionLengthMismatch represents a mismatch between distances and IDs lengths during neighbor selection.
type ErrNeighborSelectionLengthMismatch struct {
	DistancesLen int
	IDsLen       int
}

func (e *ErrNeighborSelectionLengthMismatch) Error() string {
	return fmt.Sprintf("length mismatch: got %d distances and %d IDs", e.DistancesLen, e.IDsLen)
}
