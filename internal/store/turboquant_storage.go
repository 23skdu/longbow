package store

import (
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// VectorTypeNative constants map the API-visible vector_type string values to
// internal VectorDataType constants. These are the values accepted in
// VectorSearchRequest metadata and CreateDataset parameters.
const (
	VectorTypeAPIFloat32    = "float32"
	VectorTypeAPITurboQuant = "turboquant"
	VectorTypeAPIInt8       = "int8"
	VectorTypeAPIBinary     = "binary"
)

// ParseVectorType maps an API vector_type string to the internal
// VectorDataType. An empty string defaults to float32.
//
// Returns an error with a descriptive message listing valid values if the
// type string is unrecognised.
func ParseVectorType(apiType string) (types.VectorDataType, error) {
	switch apiType {
	case "", VectorTypeAPIFloat32:
		return types.VectorTypeFloat32, nil
	case VectorTypeAPITurboQuant:
		return types.VectorTypeTQ, nil
	case VectorTypeAPIInt8:
		return types.VectorTypeInt8, nil
	case VectorTypeAPIBinary:
		return types.VectorTypeUint8, nil // Binary uses packed uint8 storage.
	default:
		return types.VectorTypeUnknown, fmt.Errorf(
			"unknown vector_type %q — valid values: %q, %q, %q, %q",
			apiType,
			VectorTypeAPIFloat32,
			VectorTypeAPITurboQuant,
			VectorTypeAPIInt8,
			VectorTypeAPIBinary,
		)
	}
}

// TrackVectorTypeCreation emits a Prometheus metric recording that a dataset
// was created with a specific vector type. Call this when a new dataset is
// first created.
func TrackVectorTypeCreation(datasetName, apiType string) {
	metrics.DatasetVectorTypeTotal.WithLabelValues(datasetName, apiType).Set(1)
}

// RecordTurboQuantEncoding records the cost and source of a TurboQuant encoding
// operation. direction should be "client_provided" or "server_encoded".
func RecordTurboQuantEncoding(datasetName, direction string, elapsed time.Duration) {
	metrics.TurboQuantEncodingTotal.WithLabelValues(datasetName, direction).Inc()
	if direction == "server_encoded" {
		metrics.TurboQuantEncodingLatencySeconds.WithLabelValues(datasetName).Observe(elapsed.Seconds())
	}
}

// UpdateTurboQuantStorageBytes updates the gauge tracking total TurboQuant
// storage bytes for a dataset (used to compare against float32 baseline).
func UpdateTurboQuantStorageBytes(datasetName string, totalBytes float64) {
	metrics.TurboQuantStorageBytesTotal.WithLabelValues(datasetName).Set(totalBytes)
}

// TurboQuantStorageRatio returns the compression ratio of a TurboQuant dataset
// relative to a float32 baseline. A ratio < 1.0 means TQ uses less storage.
//
//	ratio = tqBytes / (vectorCount * dims * 4)
func TurboQuantStorageRatio(tqBytes int64, vectorCount, dims int) float64 {
	if vectorCount == 0 || dims == 0 {
		return 0
	}
	float32Baseline := float64(vectorCount) * float64(dims) * 4
	return float64(tqBytes) / float32Baseline
}
