package store

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

// ErrDimensionLocked is returned when a vector's dimension does not match the
// locked dimension of the dataset.
var ErrDimensionLocked = errors.New("dimension locked")

// ErrDimensionMismatch is a sentinel that wraps ErrDimensionLocked with
// human-readable context. Use errors.Is(err, ErrDimensionLocked) to detect.

// DimensionGuard provides thread-safe auto-detection and locking of a
// dataset's vector dimension from the first insert.
//
// When created with dim=0 (sentinel), the first call to CheckOrSet will record
// the dimension from the incoming vector. All subsequent vectors must match.
//
// When created with dim>0 the dimension is locked immediately and
// CheckOrSet acts as a pure validator.
type DimensionGuard struct {
	mu       sync.Mutex
	dim      atomic.Int64 // 0 = not locked yet; -1 = explicit lock; >0 = dim value
	autoDetected bool
	datasetName  string
}

// NewDimensionGuard creates a guard for datasetName.
// If explicitDim > 0 the dimension is pre-locked; pass 0 to enable
// auto-detection from the first vector.
func NewDimensionGuard(datasetName string, explicitDim int) *DimensionGuard {
	g := &DimensionGuard{datasetName: datasetName}
	if explicitDim > 0 {
		g.dim.Store(int64(explicitDim))
	}
	return g
}

// CheckOrSet validates vec against the locked dimension, or, if no dimension
// is locked yet, sets the dimension from len(vec) and returns nil.
//
// Returns a descriptive *DimensionError (wrapping ErrDimensionLocked) on
// mismatch. Returns nil on success.
func (g *DimensionGuard) CheckOrSet(vec []float32) error {
	incoming := len(vec)

	// Fast path: dimension already locked.
	existing := g.dim.Load()
	if existing > 0 {
		if int(existing) == incoming {
			return nil
		}
		metrics.DatasetDimensionMismatchTotal.WithLabelValues(g.datasetName).Inc()
		return &DimensionError{
			DatasetName: g.datasetName,
			Expected:    int(existing),
			Got:         incoming,
			AutoDetected: g.autoDetected,
		}
	}

	// Slow path: first vector — lock the dimension.
	g.mu.Lock()
	defer g.mu.Unlock()

	// Re-check under lock (another goroutine may have beaten us).
	existing = g.dim.Load()
	if existing > 0 {
		if int(existing) == incoming {
			return nil
		}
		metrics.DatasetDimensionMismatchTotal.WithLabelValues(g.datasetName).Inc()
		return &DimensionError{
			DatasetName: g.datasetName,
			Expected:    int(existing),
			Got:         incoming,
			AutoDetected: g.autoDetected,
		}
	}

	// We are the first — set the dimension.
	g.dim.Store(int64(incoming))
	g.autoDetected = true
	metrics.DatasetDimensionAutoDetectTotal.WithLabelValues(g.datasetName, "success").Inc()
	return nil
}

// Dim returns the currently locked dimension (0 if not yet set).
func (g *DimensionGuard) Dim() int {
	return int(g.dim.Load())
}

// IsAutoDetected returns true if the dimension was inferred from the first
// vector rather than supplied explicitly at dataset creation.
func (g *DimensionGuard) IsAutoDetected() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.autoDetected
}

// =============================================================================
// DimensionError
// =============================================================================

// DimensionError is the rich error type returned by DimensionGuard.CheckOrSet
// on a dimension mismatch. It wraps ErrDimensionLocked so callers can use
// errors.Is.
type DimensionError struct {
	DatasetName string
	Expected    int
	Got         int
	AutoDetected bool
}

func (e *DimensionError) Error() string {
	hint := "re-create the dataset with the correct dimension"
	if e.AutoDetected {
		hint = fmt.Sprintf(
			"re-create the dataset with CreateDataset(dimension=%d) or verify your embedding model output size",
			e.Got,
		)
	}
	return fmt.Sprintf(
		"dataset %q: dimension mismatch — expected %d, received %d (hint: %s)",
		e.DatasetName, e.Expected, e.Got, hint,
	)
}

// Unwrap returns ErrDimensionLocked so errors.Is works correctly.
func (e *DimensionError) Unwrap() error {
	return ErrDimensionLocked
}
