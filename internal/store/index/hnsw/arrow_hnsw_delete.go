package hnsw

import (
	"context"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
)

// Delete runs the delete operation.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deletedMu.Lock()
	defer h.deletedMu.Unlock()
	if h.deleted == nil {
		h.deleted = roaring.New()
	}
	h.deleted.Add(id)
	h.locationStore.Delete(types.VectorID(id))
	return nil
}

// DeleteBatch runs the delete batch operation.
func (h *ArrowHNSW) DeleteBatch(ctx context.Context, ids []uint32) error {
	for _, id := range ids {
		if err := h.Delete(id); err != nil {
			return err
		}
	}
	return nil
}

// CleanupTombstones runs the cleanup tombstones operation.
func (h *ArrowHNSW) CleanupTombstones(threshold int) int {
	shouldReset := false
	totalPruned := 0
	for _, ts := range h.dataset.GetTombstones() {
		if ts == nil {
			continue
		}
		count := int(ts.Count()) // #nosec G115
		if count > threshold {
			shouldReset = true
			totalPruned = count
			break
		}
	}
	if shouldReset {
		h.dataset.ResetTombstones()
	}
	return totalPruned
}
