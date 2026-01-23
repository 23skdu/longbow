package store

// Location mapping operations extracted from arrow_hnsw_index.go

// GetLocation implements VectorIndex.
// It returns the location (batch index, row index) for a given vector ID.
func (h *ArrowHNSW) GetLocation(id VectorID) (Location, bool) {
	return h.locationStore.Get(id)
}

// GetVectorID implements VectorIndex.
// It returns the ID for a given location using the reverse index.
func (h *ArrowHNSW) GetVectorID(loc Location) (VectorID, bool) {
	return h.locationStore.GetID(loc)
}

// SetLocation allows manually setting the location for a vector ID.
// This is used by ShardedHNSW to populate shard-local location stores for filtering.
func (h *ArrowHNSW) SetLocation(id VectorID, loc Location) {
	h.locationStore.EnsureCapacity(id)
	h.locationStore.Set(id, loc)
	h.locationStore.UpdateSize(id)
}
