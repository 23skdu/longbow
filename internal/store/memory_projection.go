package store

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
)

// GetVectorTypeElementSize returns the memory size in bytes per dimension for a given vector data type.
func GetVectorTypeElementSize(dt types.VectorDataType) float64 {
	switch dt {
	case types.VectorTypeComplex128:
		return 16.0
	case types.VectorTypeComplex64, types.VectorTypeFloat64,
		types.VectorTypeInt64, types.VectorTypeUint64:
		return 8.0
	case types.VectorTypeFloat32, types.VectorTypeInt32, types.VectorTypeUint32:
		return 4.0
	case types.VectorTypeFloat16, types.VectorTypeInt16, types.VectorTypeUint16:
		return 2.0
	case types.VectorTypeInt8, types.VectorTypeUint8:
		return 1.0
	case types.VectorTypeTQ:
		return 0.5 // Standard 4-bit TurboQuant packed representation
	case types.VectorTypePQ:
		return 0.5
	case types.VectorTypeBQ:
		return 0.125 // 1 bit per dimension
	default:
		return 4.0
	}
}

// ProjectVectorMemory estimates total memory footprint for numVectors of dimension dim and data type dt,
// including raw vectors, HNSW graph structures, adjacency lists, and indexing buffers.
func ProjectVectorMemory(numVectors int64, dim int, dt types.VectorDataType) int64 {
	if numVectors <= 0 || dim <= 0 {
		return 0
	}

	elemSize := GetVectorTypeElementSize(dt)
	rawBytes := float64(numVectors) * float64(dim) * elemSize

	var graphMultiplier float64
	var perNodeOverhead float64

	switch dt {
	case types.VectorTypeTQ, types.VectorTypePQ, types.VectorTypeBQ:
		graphMultiplier = 1.3
		perNodeOverhead = 384.0
	case types.VectorTypeComplex128:
		// Complex128 (16 bytes/dim): high intermediate allocation churn during graph build
		graphMultiplier = 3.2
		perNodeOverhead = 1536.0
	case types.VectorTypeComplex64, types.VectorTypeFloat64,
		types.VectorTypeInt64, types.VectorTypeUint64:
		// 64-bit uncompressed types: 2.8x multiplier for graph + runtime GC indexing headroom
		graphMultiplier = 2.8
		perNodeOverhead = 1024.0
	default:
		graphMultiplier = 2.0
		perNodeOverhead = 512.0
	}

	projected := int64(rawBytes*graphMultiplier + float64(numVectors)*perNodeOverhead)
	if projected < 0 {
		// Overflow protection
		return 1 << 62
	}
	return projected
}

// ShouldSpillToDisk determines whether the projected memory footprint exceeds the threshold ratio
// (default 70%) of total available physical RAM or container limit.
func ShouldSpillToDisk(numVectors int64, dim int, dt types.VectorDataType, maxRAM int64, thresholdRatio float64) bool {
	if thresholdRatio <= 0 || thresholdRatio >= 1.0 {
		thresholdRatio = 0.70
	}

	if maxRAM <= 0 {
		maxRAM = memory.GetPhysicalMemory()
	}

	projected := ProjectVectorMemory(numVectors, dim, dt)
	limit := int64(float64(maxRAM) * thresholdRatio)

	return projected >= limit
}

// ShouldAutoQuantize determines whether a dataset should standardize on TurboQuant as its default
// storage mode for high-scale configurations (default 500k+ vectors) on memory-constrained infrastructure.
func ShouldAutoQuantize(numVectors int64, dim int, dt types.VectorDataType, maxRAM int64, thresholdCount int64) bool {
	// Already quantized formats do not need further auto-quantization
	if dt == types.VectorTypeTQ || dt == types.VectorTypePQ || dt == types.VectorTypeBQ {
		return false
	}

	if thresholdCount <= 0 {
		thresholdCount = 500000
	}

	if maxRAM <= 0 {
		maxRAM = memory.GetPhysicalMemory()
	}

	// Trigger on high scale (500k+) if hardware is memory constrained (<=64 GB RAM)
	// or if uncompressed projection would exceed 30% of available RAM
	if numVectors >= thresholdCount {
		if maxRAM <= 64*1024*1024*1024 {
			return true
		}
		projected := ProjectVectorMemory(numVectors, dim, dt)
		if projected >= int64(float64(maxRAM)*0.30) {
			return true
		}
	}

	return false
}
