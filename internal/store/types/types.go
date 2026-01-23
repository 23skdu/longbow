package types

// VectorDataType represents the data type of vector elements
type VectorDataType int

const (
	VectorTypeUnknown VectorDataType = iota
	VectorTypeFloat32
	VectorTypeInt8
	VectorTypeUint8
	VectorTypeFloat16
	// Add other types as needed
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
	default:
		return "unknown"
	}
}

// ElementSize returns the size in bytes of one element of this data type
func (vdt VectorDataType) ElementSize() int {
	switch vdt {
	case VectorTypeFloat32:
		return 4
	case VectorTypeInt8, VectorTypeUint8:
		return 1
	case VectorTypeFloat16:
		return 2
	default:
		return 4 // default to float32 size
	}
}

// Candidate represents a search result candidate with ID and distance
type Candidate struct {
	ID    uint32
	Dist  float32
	Level int // for hierarchical structures
}

// MaxNeighbors is the maximum number of neighbors per node in HNSW
const MaxNeighbors = 64

// ChunkSize is the size of data chunks for memory allocation
const ChunkSize = 1024

// ArrowMaxLayers is the maximum number of layers in HNSW hierarchy
const ArrowMaxLayers = 16
