package wasmclient

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
)

// Metric defines vector similarity metric for local Edge HNSW search.
type Metric string

const (
	MetricCosine Metric = "cosine"
	MetricL2     Metric = "l2"
)

// EdgeDocument holds a locally cached or edge-indexed vector and its metadata.
type EdgeDocument struct {
	ID        string         `json:"id"`
	Vector    []float32      `json:"vector"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	UpdatedAt int64          `json:"updated_at"`
	Deleted   bool           `json:"deleted,omitempty"`
}

// EdgeSearchResult represents a ranked match returned by the edge HNSW index.
type EdgeSearchResult struct {
	ID       string         `json:"id"`
	Distance float32        `json:"distance"`
	Score    float32        `json:"score"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// EdgeIndex implements a zero-dependency, WASM-friendly hierarchical small-world
// vector index for browser / Cloudflare Workers / Deno offline-first execution.
type EdgeIndex struct {
	mu     sync.RWMutex
	metric Metric
	m      int
	efCon  int
	docs   map[string]EdgeDocument
	// Simple multi-layer adjacency list by level: level -> id -> neighbors
	layers   map[int]map[string][]string
	levels   map[string]int
	entryID  string
	maxLevel int
}

// NewEdgeIndex creates a lightweight HNSW index suitable for WASM and edge environments.
func NewEdgeIndex(metric Metric, m, efConstruction int) *EdgeIndex {
	if metric == "" {
		metric = MetricCosine
	}
	if m <= 0 {
		m = 16
	}
	if efConstruction <= 0 {
		efConstruction = 64
	}
	idx := &EdgeIndex{
		metric: metric,
		m:      m,
		efCon:  efConstruction,
		docs:   make(map[string]EdgeDocument),
		layers: make(map[int]map[string][]string),
		levels: make(map[string]int),
	}
	idx.layers[0] = make(map[string][]string)
	return idx
}

func (idx *EdgeIndex) randomLevel() int {
	lvl := 0
	// P = 1/e approx 0.3678
	for rand.Float64() < 0.367879 && lvl < 6 { // #nosec G404 -- non-crypto HNSW layer sampling
		lvl++
	}
	return lvl
}

// Upsert inserts or updates a document in the local HNSW graph.
func (idx *EdgeIndex) Upsert(doc EdgeDocument) error {
	if doc.ID == "" {
		return errors.New("document id is required")
	}
	if len(doc.Vector) == 0 && !doc.Deleted {
		return errors.New("document vector cannot be empty")
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	_, existed := idx.docs[doc.ID]
	vecCopy := make([]float32, len(doc.Vector))
	copy(vecCopy, doc.Vector)
	doc.Vector = vecCopy
	idx.docs[doc.ID] = doc

	if doc.Deleted {
		return nil
	}
	if existed {
		// Re-link neighbors cleanly on update
		idx.pruneLinksLocked(doc.ID)
	}

	lvl := idx.randomLevel()
	idx.levels[doc.ID] = lvl
	for l := 0; l <= lvl; l++ {
		if idx.layers[l] == nil {
			idx.layers[l] = make(map[string][]string)
		}
		if _, ok := idx.layers[l][doc.ID]; !ok {
			idx.layers[l][doc.ID] = nil
		}
	}

	if idx.entryID == "" || idx.docs[idx.entryID].Deleted {
		idx.entryID = doc.ID
		idx.maxLevel = lvl
		return nil
	}

	cur := idx.entryID
	for l := idx.maxLevel; l > lvl; l-- {
		cur = idx.greedyStepLocked(doc.Vector, cur, l)
	}

	minLvl := lvl
	if idx.maxLevel < minLvl {
		minLvl = idx.maxLevel
	}
	for l := minLvl; l >= 0; l-- {
		candidates := idx.searchLayerLocked(doc.Vector, cur, idx.efCon, l)
		neighbors := make([]string, 0, idx.m)
		for _, c := range candidates {
			if c.id == doc.ID {
				continue
			}
			neighbors = append(neighbors, c.id)
			if len(neighbors) >= idx.m {
				break
			}
		}
		idx.layers[l][doc.ID] = neighbors
		for _, nb := range neighbors {
			adj := idx.layers[l][nb]
			adj = append(adj, doc.ID)
			if len(adj) > idx.m*2 {
				adj = adj[len(adj)-idx.m*2:]
			}
			idx.layers[l][nb] = adj
		}
		if len(candidates) > 0 {
			cur = candidates[0].id
		}
	}

	if lvl > idx.maxLevel {
		idx.maxLevel = lvl
		idx.entryID = doc.ID
	}
	return nil
}

// Delete marks a document deleted in the edge index.
func (idx *EdgeIndex) Delete(id string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	doc, ok := idx.docs[id]
	if !ok {
		return false
	}
	doc.Deleted = true
	idx.docs[id] = doc
	return true
}

// Len returns active (non-deleted) documents in the edge index.
func (idx *EdgeIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	count := 0
	for _, d := range idx.docs {
		if !d.Deleted {
			count++
		}
	}
	return count
}

type distNode struct {
	id   string
	dist float32
}

// Search performs hierarchical HNSW approximate nearest-neighbor search in pure Go/WASM.
func (idx *EdgeIndex) Search(query []float32, k int) ([]EdgeSearchResult, error) {
	if len(query) == 0 {
		return nil, errors.New("query vector cannot be empty")
	}
	if k <= 0 {
		k = 10
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.entryID == "" || len(idx.docs) == 0 {
		return nil, nil
	}

	// Small-N exact fallback keeps edge recall at 1.0 when few items are cached locally.
	if len(idx.docs) <= 128 {
		return idx.exactSearchLocked(query, k), nil
	}

	cur := idx.entryID
	for l := idx.maxLevel; l > 0; l-- {
		cur = idx.greedyStepLocked(query, cur, l)
	}
	ef := k * 4
	if ef < idx.efCon {
		ef = idx.efCon
	}
	cands := idx.searchLayerLocked(query, cur, ef, 0)
	res := make([]EdgeSearchResult, 0, min(k, len(cands)))
	for _, c := range cands {
		doc := idx.docs[c.id]
		if doc.Deleted {
			continue
		}
		res = append(res, EdgeSearchResult{
			ID:       doc.ID,
			Distance: c.dist,
			Score:    1.0 / (1.0 + c.dist),
			Metadata: doc.Metadata,
		})
		if len(res) >= k {
			break
		}
	}
	return res, nil
}

func (idx *EdgeIndex) exactSearchLocked(query []float32, k int) []EdgeSearchResult {
	var all []distNode
	for id, doc := range idx.docs {
		if doc.Deleted || len(doc.Vector) != len(query) {
			continue
		}
		all = append(all, distNode{id: id, dist: computeEdgeDistance(idx.metric, query, doc.Vector)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
	if len(all) > k {
		all = all[:k]
	}
	out := make([]EdgeSearchResult, len(all))
	for i, c := range all {
		doc := idx.docs[c.id]
		out[i] = EdgeSearchResult{
			ID:       doc.ID,
			Distance: c.dist,
			Score:    1.0 / (1.0 + c.dist),
			Metadata: doc.Metadata,
		}
	}
	return out
}

func (idx *EdgeIndex) greedyStepLocked(query []float32, cur string, level int) string {
	bestDist := idx.distToIDLocked(query, cur)
	improved := true
	for improved {
		improved = false
		for _, nb := range idx.layers[level][cur] {
			d := idx.distToIDLocked(query, nb)
			if d < bestDist {
				bestDist = d
				cur = nb
				improved = true
			}
		}
	}
	return cur
}

func (idx *EdgeIndex) searchLayerLocked(query []float32, entry string, ef, level int) []distNode {
	visited := map[string]struct{}{entry: {}}
	dEntry := idx.distToIDLocked(query, entry)
	frontier := []distNode{{id: entry, dist: dEntry}}
	best := []distNode{{id: entry, dist: dEntry}}

	for len(frontier) > 0 {
		sort.Slice(frontier, func(i, j int) bool { return frontier[i].dist < frontier[j].dist })
		cur := frontier[0]
		frontier = frontier[1:]

		worstBest := best[len(best)-1].dist
		if len(best) >= ef && cur.dist > worstBest {
			break
		}
		for _, nb := range idx.layers[level][cur.id] {
			if _, seen := visited[nb]; seen {
				continue
			}
			visited[nb] = struct{}{}
			d := idx.distToIDLocked(query, nb)
			if len(best) < ef || d < worstBest {
				node := distNode{id: nb, dist: d}
				frontier = append(frontier, node)
				best = append(best, node)
				sort.Slice(best, func(i, j int) bool { return best[i].dist < best[j].dist })
				if len(best) > ef {
					best = best[:ef]
				}
				worstBest = best[len(best)-1].dist
			}
		}
	}
	return best
}

func (idx *EdgeIndex) distToIDLocked(query []float32, id string) float32 {
	doc, ok := idx.docs[id]
	if !ok || len(doc.Vector) != len(query) {
		return float32(math.MaxFloat32)
	}
	return computeEdgeDistance(idx.metric, query, doc.Vector)
}

func (idx *EdgeIndex) pruneLinksLocked(id string) {
	for _, layer := range idx.layers {
		delete(layer, id)
	}
}

func computeEdgeDistance(metric Metric, a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return float32(math.MaxFloat32)
	}
	if metric == MetricL2 {
		var sum float64
		for i := range a {
			d := float64(a[i] - b[i])
			sum += d * d
		}
		return float32(math.Sqrt(sum))
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	cos := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	if cos > 1.0 {
		cos = 1.0
	} else if cos < -1.0 {
		cos = -1.0
	}
	return float32(1.0 - cos)
}

// ExportJSON serializes local edge index documents for IndexedDB/KV persistence.
func (idx *EdgeIndex) ExportJSON() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return json.Marshal(struct {
		Metric Metric         `json:"metric"`
		Docs   []EdgeDocument `json:"docs"`
	}{
		Metric: idx.metric,
		Docs:   idx.sortedDocsLocked(),
	})
}

func (idx *EdgeIndex) sortedDocsLocked() []EdgeDocument {
	out := make([]EdgeDocument, 0, len(idx.docs))
	for _, d := range idx.docs {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ImportJSON restores serialized edge index documents and rebuilds the HNSW graph.
func (idx *EdgeIndex) ImportJSON(data []byte) error {
	var snap struct {
		Metric Metric         `json:"metric"`
		Docs   []EdgeDocument `json:"docs"`
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	for _, doc := range snap.Docs {
		if err := idx.Upsert(doc); err != nil {
			return err
		}
	}
	return nil
}
