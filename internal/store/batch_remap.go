package store

// BatchRemapInfo contains information about a batch of vectors being remapped to new locations.
type BatchRemapInfo struct {
	NewBatchIdx int
	NewRowIdxs  []int
}
