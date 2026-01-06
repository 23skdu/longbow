package store

import "github.com/23skdu/longbow/internal/core"

// Type Aliases to maintain backward compatibility while migrating to internal/core

// VectorID is a unique identifier for a vector in the system.
type VectorID = core.VectorID

// Location maps a VectorID to a physical location in a Dataset (Batch + Row).
type Location = core.Location

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

// Errors (Aliased from internal/core)

// ErrNotFound indicates a requested resource does not exist.
type ErrNotFound = core.ErrNotFound

// NewNotFoundError creates a not found error.
var NewNotFoundError = core.NewNotFoundError
