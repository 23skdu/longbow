package store

import "github.com/apache/arrow-go/v18/arrow"

// getRecordBatch retrieves the appropriate record batch for a given index during bulk insert.
// It abstracts the logic for handling direct (1:1) vs batched mapping.
func getRecordBatch(i int, recs []arrow.RecordBatch, batchIdxs []int, useDirectIndex bool) arrow.RecordBatch {
	if useDirectIndex {
		return recs[i]
	}
	if i < len(batchIdxs) {
		bIdx := batchIdxs[i]
		if bIdx < len(recs) {
			return recs[bIdx]
		}
	}
	// Fallback to first batch if available, logic matches original repeating pattern
	if len(recs) > 0 {
		return recs[0]
	}
	return nil
}
