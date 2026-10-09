package index

import (
	"github.com/23skdu/longbow/internal/store/types"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
)

// types.Location mapping operations extracted from arrow_hnsw_index.go

// GetLocation implements VectorIndex.
// It returns the location (batch index, row index) for a given vector ID.
func (h *ArrowHNSW) GetLocation(id uint32) (any, bool) {
	if h.locationStore == nil {
		return nil, false
	}
	return h.locationStore.Get(types.VectorID(id))
}

// GetVectorID implements VectorIndex.
// It returns the ID for a given location using the reverse index.
func (h *ArrowHNSW) GetVectorID(loc any) (uint32, bool) {
	if h.locationStore == nil {
		return 0, false
	}
	l, ok := loc.(types.Location)
	if !ok {
		return 0, false
	}
	id, ok := h.locationStore.GetID(l)
	return uint32(id), ok
}

// SetLocation allows manually setting the location for a vector ID.
// This is used by ShardedHNSW to populate shard-local location stores for filtering.
func (h *ArrowHNSW) SetLocation(id types.VectorID, loc types.Location) {
	if h.locationStore == nil {
		return
	}
	h.locationStore.EnsureCapacity(id)
	h.locationStore.Set(id, loc)
	h.locationStore.UpdateSize(id)
}

// IndexExternalID records the mapping from an external (client-visible) ID
// to an internal uint32 node ID. Called during insert to enable O(1) reverse lookups.
func (h *ArrowHNSW) IndexExternalID(externalID uint64, internalID uint32) {
	h.externalIDIndexMu.Lock()
	if h.externalIDIndex == nil {
		h.externalIDIndex = make(map[uint64]uint32)
	}
	if h.internalToExternalID == nil {
		h.internalToExternalID = make(map[uint32]uint64)
	}
	h.externalIDIndex[externalID] = internalID
	h.internalToExternalID[internalID] = externalID
	h.externalIDIndexMu.Unlock()
}

// LookupInternalID returns the internal node ID for a given external ID,
// or (0, false) if not found.
func (h *ArrowHNSW) LookupInternalID(externalID uint64) (uint32, bool) {
	h.externalIDIndexMu.RLock()
	id, ok := h.externalIDIndex[externalID]
	h.externalIDIndexMu.RUnlock()
	return id, ok
}

// LookupExternalID returns the external client ID for a given internal node ID.
// Returns (0, false) if no mapping can be resolved.
func (h *ArrowHNSW) LookupExternalID(internalID uint32) (uint64, bool) {
	h.externalIDIndexMu.RLock()
	if extID, ok := h.internalToExternalID[internalID]; ok {
		h.externalIDIndexMu.RUnlock()
		return extID, true
	}
	h.externalIDIndexMu.RUnlock()

	// Fallback: consult batch location store to read column 0 of original record batch.
	locAny, ok := h.GetLocation(internalID)
	if !ok {
		return 0, false
	}
	loc, ok := locAny.(types.Location)
	if !ok {
		return 0, false
	}
	if h.dataset != nil {
		records := h.dataset.GetRecords()
		if loc.BatchIdx < len(records) {
			rec := records[loc.BatchIdx]
			if rec.NumCols() > 0 && loc.RowIdx < int(rec.NumRows()) {
				switch col := rec.Column(0).(type) {
				case *arrowarray.Int64:
					if loc.RowIdx < col.Len() {
						val := col.Value(loc.RowIdx)
						if val >= 0 {
							ext := uint64(val)
							h.IndexExternalID(ext, internalID)
							return ext, true
						}
					}
				case *arrowarray.Uint64:
					if loc.RowIdx < col.Len() {
						val := col.Value(loc.RowIdx)
						h.IndexExternalID(val, internalID)
						return val, true
					}
				}
			}
		}
	}
	return 0, false
}

// RemoveExternalID removes the external ID mapping for a given external ID.
func (h *ArrowHNSW) RemoveExternalID(externalID uint64) {
	h.externalIDIndexMu.Lock()
	if internalID, ok := h.externalIDIndex[externalID]; ok {
		delete(h.internalToExternalID, internalID)
	}
	delete(h.externalIDIndex, externalID)
	h.externalIDIndexMu.Unlock()
}
