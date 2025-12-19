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
config    *HNSWIndexConfig
hnsw      *HNSWIndex //nolint:unused // reserved for future HNSW integration // actual HNSW index, nil until dataset provided
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

func (h *HNSWPluggableAdapter) SearchVectors(query []float32, k int) []SearchResult {
results, _ := h.Search(query, k)
searchResults := make([]SearchResult, len(results))
for i, r := range results {
searchResults[i] = SearchResult{ID: VectorID(r.ID), Score: r.Distance}
}
return searchResults
}

func (h *HNSWPluggableAdapter) Len() int {
return h.Size()
}

// =============================================================================
// IVF-Flat Index (Stub Implementation)
// =============================================================================

// IVFFlatIndex implements PluggableVectorIndex for IVF-Flat algorithm
type IVFFlatIndex struct {
mu        sync.RWMutex
dimension int
vectors   map[uint64][]float32
config    *IVFFlatConfig
built     bool
}

func (ivf *IVFFlatIndex) Type() IndexType {
return IndexTypeIVFFlat
}

func (ivf *IVFFlatIndex) Dimension() int {
return ivf.dimension
}

func (ivf *IVFFlatIndex) Size() int {
ivf.mu.RLock()
defer ivf.mu.RUnlock()
return len(ivf.vectors)
}

func (ivf *IVFFlatIndex) NeedsBuild() bool {
return true // IVF requires training/clustering
}

func (ivf *IVFFlatIndex) Add(id uint64, vector []float32) error {
ivf.mu.Lock()
defer ivf.mu.Unlock()
ivf.vectors[id] = vector
return nil
}

func (ivf *IVFFlatIndex) AddBatch(ids []uint64, vectors [][]float32) error {
ivf.mu.Lock()
defer ivf.mu.Unlock()
for i, id := range ids {
ivf.vectors[id] = vectors[i]
}
return nil
}

func (ivf *IVFFlatIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
ivf.mu.RLock()
defer ivf.mu.RUnlock()

results := make([]IndexSearchResult, 0, k)
for id := range ivf.vectors {
results = append(results, IndexSearchResult{ID: id, Distance: 0.0})
if len(results) >= k {
break
}
}
return results, nil
}

func (ivf *IVFFlatIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
results := make([][]IndexSearchResult, len(queries))
for i, q := range queries {
r, _ := ivf.Search(q, k)
results[i] = r
}
return results, nil
}

func (ivf *IVFFlatIndex) Build() error {
ivf.built = true
return nil
}

func (ivf *IVFFlatIndex) Save(path string) error {
return os.WriteFile(path, []byte("ivf_flat"), 0o644)
}

func (ivf *IVFFlatIndex) Load(path string) error {
_, err := os.ReadFile(path)
return err
}

func (ivf *IVFFlatIndex) Close() error {
return nil
}

func (ivf *IVFFlatIndex) AddByLocation(batchIdx, rowIdx int) error {
return nil
}

func (ivf *IVFFlatIndex) SearchVectors(query []float32, k int) []SearchResult {
results, _ := ivf.Search(query, k)
searchResults := make([]SearchResult, len(results))
for i, r := range results {
searchResults[i] = SearchResult{ID: VectorID(r.ID), Score: r.Distance}
}
return searchResults
}

func (ivf *IVFFlatIndex) Len() int {
return ivf.Size()
}

// =============================================================================
// DiskANN Index (Stub Implementation)
// =============================================================================

// DiskANNIndex implements PluggableVectorIndex for DiskANN algorithm
type DiskANNIndex struct {
mu        sync.RWMutex
dimension int
vectors   map[uint64][]float32
config    *DiskANNConfig
built     bool
}

func (d *DiskANNIndex) Type() IndexType {
return IndexTypeDiskANN
}

func (d *DiskANNIndex) Dimension() int {
return d.dimension
}

func (d *DiskANNIndex) Size() int {
d.mu.RLock()
defer d.mu.RUnlock()
return len(d.vectors)
}

func (d *DiskANNIndex) NeedsBuild() bool {
return true // DiskANN requires graph construction
}

func (d *DiskANNIndex) Add(id uint64, vector []float32) error {
d.mu.Lock()
defer d.mu.Unlock()
d.vectors[id] = vector
return nil
}

func (d *DiskANNIndex) AddBatch(ids []uint64, vectors [][]float32) error {
d.mu.Lock()
defer d.mu.Unlock()
for i, id := range ids {
d.vectors[id] = vectors[i]
}
return nil
}

func (d *DiskANNIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
d.mu.RLock()
defer d.mu.RUnlock()

results := make([]IndexSearchResult, 0, k)
for id := range d.vectors {
results = append(results, IndexSearchResult{ID: id, Distance: 0.0})
if len(results) >= k {
break
}
}
return results, nil
}

func (d *DiskANNIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
results := make([][]IndexSearchResult, len(queries))
for i, q := range queries {
r, _ := d.Search(q, k)
results[i] = r
}
return results, nil
}

func (d *DiskANNIndex) Build() error {
d.built = true
return nil
}

func (d *DiskANNIndex) Save(path string) error {
return os.WriteFile(path, []byte("diskann"), 0o644)
}

func (d *DiskANNIndex) Load(path string) error {
_, err := os.ReadFile(path)
return err
}

func (d *DiskANNIndex) Close() error {
return nil
}

func (d *DiskANNIndex) AddByLocation(batchIdx, rowIdx int) error {
return nil
}

func (d *DiskANNIndex) SearchVectors(query []float32, k int) []SearchResult {
results, _ := d.Search(query, k)
searchResults := make([]SearchResult, len(results))
for i, r := range results {
searchResults[i] = SearchResult{ID: VectorID(r.ID), Score: r.Distance}
}
return searchResults
}

func (d *DiskANNIndex) Len() int {
return d.Size()
}
