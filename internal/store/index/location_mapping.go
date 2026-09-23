package index

import (
	"github.com/23skdu/longbow/internal/store/types"
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
	h.externalIDIndex[externalID] = internalID
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

// RemoveExternalID removes the external ID mapping for a given internal ID.
func (h *ArrowHNSW) RemoveExternalID(externalID uint64) {
	h.externalIDIndexMu.Lock()
	delete(h.externalIDIndex, externalID)
	h.externalIDIndexMu.Unlock()
}
