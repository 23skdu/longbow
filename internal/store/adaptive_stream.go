package store

import (
	"github.com/23skdu/longbow/internal/flight"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

// AdaptivelySliceBatches slices a list of record batches into smaller chunks
// based on the provided AdaptiveChunkStrategy. It handles proper slicing of
// associated tombstones and reassigns sequential batch indices.
//
// The returned batches are Retained and must be Released by the caller.
// The returned tombstones map uses the new sequential indices.
func AdaptivelySliceBatches(
	records []arrow.RecordBatch,
	tombstones map[int]*query.Bitset,
	strategy *flight.AdaptiveChunkStrategy,
) ([]arrow.RecordBatch, map[int]*query.Bitset) {
	var outRecords []arrow.RecordBatch
	outTombstones := make(map[int]*query.Bitset)
	nextIdx := 0

	// Track total adaptive chunks for metrics
	chunkCount := 0

	for i, rec := range records {
		originalRows := rec.NumRows()
		offset := int64(0)

		// Get tombstone for this original batch
		ts := tombstones[i]

		for offset < originalRows {
			// Determine next chunk size from strategy
			targetSize := int64(strategy.NextChunkSize())
			remaining := originalRows - offset

			if targetSize > remaining {
				targetSize = remaining
			}

			// Slice the record
			// NewSlice retains the underlying arrays, so the new batch is valid independently
			sliced := rec.NewSlice(offset, offset+targetSize)
			outRecords = append(outRecords, sliced)

			// Update metrics
			metrics.DoGetChunkSizeHistogram.Observe(float64(targetSize))
			chunkCount++

			// Handle tombstone slicing if present
			if ts != nil {
				// Slice returns a new Bitset with bits shifted relative to the offset
				subTs := ts.Slice(int(offset), int(targetSize))
				// Only store if there are set bits (optimizations elsewhere skip empty tombstones)
				if subTs.Count() > 0 {
					outTombstones[nextIdx] = subTs
				}
			}

			nextIdx++
			offset += targetSize
		}
	}

	metrics.DoGetAdaptiveChunksTotal.Add(float64(chunkCount))

	// Record current growth rate
	if strategy != nil {
		// Just an approximation or we can expose it in strategy
		// For now we skip tricky math, maybe just current size
		// metrics.DoGetChunkGrowthRate.Set(float64(strategy.CurrentSize()))
	}

	return outRecords, outTombstones
}
