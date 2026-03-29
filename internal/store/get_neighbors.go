package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
)

// ErrVectorNotFound is returned by LookupNeighbors when the requested
// external ID does not exist in the index.
var ErrVectorNotFound = errors.New("vector not found")

// ErrGetNeighborsNotSupported is returned by index types that do not expose
// pre-computed neighborhood lists (e.g. types not implementing the low-level
// neighbor lookup).
//
// Callers that receive this sentinel should fall back to:
//
//	SearchVectors with the stored vector as the query.
var ErrGetNeighborsNotSupported = errors.New(
	"GetNeighbors is not supported by this index type — " +
		"use SearchVectors with a stored vector as query instead",
)

// NeighborResult holds a single neighbour from a LookupNeighbors call.
type NeighborResult struct {
	ID       uint64
	Distance float32
}

// LookupNeighbors retrieves the pre-computed HNSW layer-0 neighbors of a stored
// vector identified by externalID (the client-visible ID in the Arrow id column).
//
// Only ArrowHNSW-backed datasets support this operation. All other index types
// return ErrGetNeighborsNotSupported.
//
// k limits the result set (0 = return all stored neighbors).
//
// Prometheus metrics are emitted on all code paths.
func LookupNeighbors(ctx context.Context, ds *Dataset, externalID uint64, k int) ([]NeighborResult, error) {
	if ds == nil {
		return nil, fmt.Errorf("LookupNeighbors: nil dataset")
	}

	start := time.Now()

	switch idx := ds.Index.(type) {
	case *ArrowHNSW:
		result, err := arrowHNSWLookupNeighbors(idx, externalID, k)
		elapsed := time.Since(start)
		emitGetNeighborsMetrics(ds.Name, "hnsw", err, elapsed, len(result))
		return result, err

	default:
		indexTypeLabel := fmt.Sprintf("%T", ds.Index)
		metrics.GetNeighborsTotal.WithLabelValues(ds.Name, indexTypeLabel, "not_supported").Inc()
		return nil, ErrGetNeighborsNotSupported
	}
}

// arrowHNSWLookupNeighbors resolves the externalID to an internal node uint32,
// delegates to the existing ArrowHNSW.GetNeighbors method, and computes
// distances for each returned neighbor ID.
func arrowHNSWLookupNeighbors(h *ArrowHNSW, externalID uint64, k int) ([]NeighborResult, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("ArrowHNSW: graph data not initialized")
	}

	// Resolve external -> internal ID.
	internalID, ok := resolveInternalID(h, externalID)
	if !ok {
		return nil, fmt.Errorf("%w: id=%d", ErrVectorNotFound, externalID)
	}

	// Delegate to the existing GetNeighbors method on ArrowHNSW.
	neighborIDs, err := h.GetNeighbors(internalID)
	if err != nil {
		return nil, err
	}
	if len(neighborIDs) == 0 {
		return nil, nil
	}

	// Apply k limit before distance computation.
	if k > 0 && len(neighborIDs) > k {
		neighborIDs = neighborIDs[:k]
	}

	results := make([]NeighborResult, 0, len(neighborIDs))
	srcVec, _ := data.GetVector(internalID)
	for _, nbrID := range neighborIDs {
		dist := float32(0)
		nbrVec, _ := data.GetVector(nbrID)
		if srcF32, ok := srcVec.([]float32); ok {
			if nbrF32, ok := nbrVec.([]float32); ok {
				d, dErr := h.distFunc(srcF32, nbrF32)
				if dErr == nil {
					dist = d
				}
			}
		}
		results = append(results, NeighborResult{
			ID:       uint64(nbrID),
			Distance: dist,
		})
	}

	return results, nil
}

// resolveInternalID maps an external client ID to the ArrowHNSW internal
// uint32 node ID by consulting the ChunkedLocationStore and the Arrow record.
func resolveInternalID(h *ArrowHNSW, externalID uint64) (uint32, bool) {
	nodeCount := h.nodeCount.Load()
	if nodeCount < 0 {
		return 0, false
	}
	// Cap count at MaxUint32 for safe iteration if needed, though uint32
	// is the internal node ID limit.
	var count uint32
	if nodeCount > 0xFFFFFFFF {
		count = 0xFFFFFFFF
	} else {
		count = uint32(nodeCount)
	}

	for internalID := uint32(0); internalID < count; internalID++ {
		locAny, ok := h.GetLocation(internalID)
		if !ok {
			continue
		}
		loc, ok := locAny.(Location)
		if !ok {
			continue
		}

		if h.dataset == nil || loc.BatchIdx >= len(h.dataset.Records) {
			// Fallback: treat internal ID as external ID.
			if uint64(internalID) == externalID {
				return internalID, true
			}
			continue
		}

		rec := h.dataset.Records[loc.BatchIdx]
		if rec.NumCols() == 0 || loc.RowIdx >= int(rec.NumRows()) {
			continue
		}
		// id column is assumed to be column 0, type Int64.
		if int64Col, ok := rec.Column(0).(*arrowarray.Int64); ok && loc.RowIdx < int64Col.Len() {
			val := int64Col.Value(loc.RowIdx)
			// Only compare if non-negative to avoid wrap-around issues with uint64 cast.
			if val >= 0 && uint64(val) == externalID {
				return internalID, true
			}
		}
	}
	return 0, false
}

// emitGetNeighborsMetrics emits the three GetNeighbors Prometheus metrics.
func emitGetNeighborsMetrics(dataset, indexType string, err error, elapsed time.Duration, resultCount int) {
	label := resultLabel(err)
	metrics.GetNeighborsTotal.WithLabelValues(dataset, indexType, label).Inc()
	metrics.GetNeighborsLatencySeconds.WithLabelValues(dataset, indexType).Observe(elapsed.Seconds())
	if err == nil {
		metrics.GetNeighborsResultSize.WithLabelValues(dataset).Observe(float64(resultCount))
	}
}

// resultLabel converts an error to a short Prometheus cardinality-safe label.
func resultLabel(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrVectorNotFound):
		return "not_found"
	case errors.Is(err, ErrGetNeighborsNotSupported):
		return "not_supported"
	default:
		return "error"
	}
}
