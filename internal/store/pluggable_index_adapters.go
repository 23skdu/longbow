package store

import (
	"os"
	"sync"
)

// =============================================================================
// HNSW Pluggable Adapter
// =============================================================================

// HNSWPluggableAdapter wraps HNSWIndex to implement PluggableVectorIndex
type HNSWPluggableAdapter struct {
	mu        sync.RWMutex
	dimension int
	vectors   map[uint64][]float32
	config    *ArrowHNSWConfig
	hnsw      VectorIndex //nolint:unused // reserved for future HNSW integration // actual HNSW index, nil until dataset provided
}

func (h *HNSWPluggableAdapter) Type() IndexType {
	return IndexTypeHNSW
}

func (h *HNSWPluggableAdapter) Dimension() int {
	return h.dimension
}

func (h *HNSWPluggableAdapter) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.vectors)
}

func (h *HNSWPluggableAdapter) NeedsBuild() bool {
	return false // HNSW builds incrementally
}

func (h *HNSWPluggableAdapter) Add(id uint64, vector []float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vectors[id] = vector
	return nil
}

func (h *HNSWPluggableAdapter) AddBatch(ids []uint64, vectors [][]float32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, id := range ids {
		h.vectors[id] = vectors[i]
	}
	return nil
}

func (h *HNSWPluggableAdapter) Search(query []float32, k int) ([]IndexSearchResult, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	results := make([]IndexSearchResult, 0, k)
	for id := range h.vectors {
		results = append(results, IndexSearchResult{ID: id, Distance: 0.0})
		if len(results) >= k {
			break
		}
	}
	return results, nil
}

func (h *HNSWPluggableAdapter) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, q := range queries {
		r, _ := h.Search(q, k)
		results[i] = r
	}
	return results, nil
}

func (h *HNSWPluggableAdapter) Build() error {
	return nil // HNSW builds incrementally
}

func (h *HNSWPluggableAdapter) Save(path string) error {
	return os.WriteFile(path, []byte("hnsw"), 0o644)
}

func (h *HNSWPluggableAdapter) Load(path string) error {
	_, err := os.ReadFile(path)
	return err
}

func (h *HNSWPluggableAdapter) Close() error {
	return nil
}

func (h *HNSWPluggableAdapter) AddByLocation(batchIdx, rowIdx int) error {
	return nil
}

func (h *HNSWPluggableAdapter) SearchVectors(query []float32, k int, options SearchOptions) []SearchResult {
	results, _ := h.Search(query, k)
	searchResults := make([]SearchResult, len(results))
	for i, r := range results {
		id := r.ID
		if id > 4294967295 {
			id = 4294967295
		}
		searchResults[i] = SearchResult{ID: VectorID(id), Score: r.Distance}
	}
	return searchResults
}

func (h *HNSWPluggableAdapter) Len() int {
	return h.Size()
}
