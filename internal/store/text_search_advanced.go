package store

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdvancedTextSearchManager provides full-text boolean/phrase/fuzzy search, highlighting,
// faceted counts, dynamic BM25 k1/b tuning, and search analytics tracking.
type AdvancedTextSearchManager struct {
	mu           sync.RWMutex
	store        *VectorStore
	datasetBM25  map[string]BM25TuningParams
	corpusCache  map[string]*cachedTextCorpus
	totalQueries int64
	zeroQueries  int64
	analytics    map[string]*QueryAnalyticsEntry
}

// NewAdvancedTextSearchManager creates an advanced text search & analytics engine.
func NewAdvancedTextSearchManager(s *VectorStore) *AdvancedTextSearchManager {
	return &AdvancedTextSearchManager{
		store:       s,
		datasetBM25: make(map[string]BM25TuningParams),
		corpusCache: make(map[string]*cachedTextCorpus),
		analytics:   make(map[string]*QueryAnalyticsEntry),
	}
}

// SetBM25Params tunes k1 and b parameters dynamically for a dataset.
func (m *AdvancedTextSearchManager) SetBM25Params(dataset string, params BM25TuningParams) error {
	if params.K1 < 0 || params.B < 0 || params.B > 1 {
		return fmt.Errorf("invalid BM25 params: k1=%v b=%v (require k1>=0, 0<=b<=1)", params.K1, params.B)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.datasetBM25[dataset] = params
	return nil
}

// GetBM25Params returns effective BM25 tuning for dataset.
func (m *AdvancedTextSearchManager) GetBM25Params(dataset string) BM25TuningParams {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if p, ok := m.datasetBM25[dataset]; ok {
		return p
	}
	return DefaultBM25Tuning()
}

// Search executes full-text boolean/phrase/fuzzy evaluation with tunable BM25 scoring,
// highlighted snippets, faceted counts, and analytics recording.
func (m *AdvancedTextSearchManager) Search(ctx context.Context, req AdvancedTextQueryRequest) (*AdvancedTextSearchResponse, error) {
	start := time.Now()
	_ = ctx
	if req.Dataset == "" {
		return nil, errors.New("dataset is required")
	}
	ast, err := ParseAdvancedTextQuery(req.Query)
	if err != nil {
		return nil, err
	}
	if req.K <= 0 {
		req.K = 10
	}
	bm25 := m.GetBM25Params(req.Dataset)
	if req.BM25 != nil {
		bm25 = *req.BM25
	}
	openTag := "<mark>"
	closeTag := "</mark>"
	if req.HighlightTag != "" {
		openTag = "<" + req.HighlightTag + ">"
		closeTag = "</" + req.HighlightTag + ">"
	}

	corpus := &cachedTextCorpus{avgDL: 1.0, docBase: []int{0}}
	if m.store != nil {
		var err error
		corpus, err = m.corpusFor(req.Dataset)
		if err != nil {
			m.recordAnalytics(req.Query, 0, time.Since(start))
			return nil, err
		}
	}

	th := &textCandidateHeap{k: req.K}
	facetMaps := make(map[string]map[string]int)
	for _, f := range req.FacetFields {
		facetMaps[f] = make(map[string]int)
	}

	totalHits := evaluateTextCorpus(corpus, ast, req, bm25, th, facetMaps)

	// Extract top-K from heap (min-heap → descending).
	heap.Init(th)
	sorted := make([]textCandidate, th.Len())
	for i := th.Len() - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(th).(textCandidate)
	}

	hits := make([]HighlightedTextHit, len(sorted))
	for i, c := range sorted {
		d := corpus.docAt(c.docIdx)
		if d == nil {
			continue
		}
		// Re-evaluate only the retained hits: keeping matched terms for every
		// candidate made a broad query retain one slice per matching document.
		_, mt := ast.evaluate(d.lowerText, d.tokens, req.FuzzyMaxDist)
		snippet, hl := GenerateSnippetHighlight(d.text, mt, openTag, closeTag, req.SnippetChars)
		hits[i] = HighlightedTextHit{
			ID:         d.id,
			VectorID:   d.vectorID,
			Score:      c.score,
			Snippet:    snippet,
			Highlights: hl,
			Metadata:   d.meta,
		}
	}

	var facets map[string][]FacetCount
	if len(req.FacetFields) > 0 {
		facets = make(map[string][]FacetCount, len(req.FacetFields))
		for ff, counts := range facetMaps {
			list := make([]FacetCount, 0, len(counts))
			for val, cnt := range counts {
				list = append(list, FacetCount{Value: val, Count: cnt})
			}
			sort.Slice(list, func(i, j int) bool {
				if list[i].Count == list[j].Count {
					return list[i].Value < list[j].Value
				}
				return list[i].Count > list[j].Count
			})
			facets[ff] = list
		}
	}

	elapsed := time.Since(start)
	m.recordAnalytics(req.Query, totalHits, elapsed)

	return &AdvancedTextSearchResponse{
		Query:     req.Query,
		TotalHits: totalHits,
		TookMs:    float64(elapsed.Microseconds()) / 1000.0,
		Hits:      hits,
		Facets:    facets,
		BM25Used:  bm25,
	}, nil
}

// corpusFor returns the cached, sharded corpus for dataset, rebuilding only the
// record batches whose contents or tombstones changed.

// Analytics runs the analytics operation.
func (m *AdvancedTextSearchManager) Analytics(limit int) SearchAnalyticsSnapshot {
	if limit <= 0 {
		limit = 20
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	all := make([]QueryAnalyticsEntry, 0, len(m.analytics))
	var zeroOnly []QueryAnalyticsEntry
	for _, e := range m.analytics {
		all = append(all, *e)
		if e.ZeroResults > 0 {
			zeroOnly = append(zeroOnly, *e)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Count > all[j].Count })
	sort.Slice(zeroOnly, func(i, j int) bool { return zeroOnly[i].ZeroResults > zeroOnly[j].ZeroResults })
	if len(all) > limit {
		all = all[:limit]
	}
	if len(zeroOnly) > limit {
		zeroOnly = zeroOnly[:limit]
	}
	return SearchAnalyticsSnapshot{
		TotalQueries:      m.totalQueries,
		ZeroResultQueries: m.zeroQueries,
		TopQueries:        all,
		ZeroResultTop:     zeroOnly,
	}
}

func (m *AdvancedTextSearchManager) recordAnalytics(query string, hits int, dur time.Duration) {
	norm := strings.TrimSpace(strings.ToLower(query))
	if norm == "" {
		return
	}
	ms := float64(dur.Microseconds()) / 1000.0
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalQueries++
	if hits == 0 {
		m.zeroQueries++
	}
	ent, ok := m.analytics[norm]
	if !ok {
		ent = &QueryAnalyticsEntry{Query: norm}
		m.analytics[norm] = ent
	}
	ent.AvgLatencyMs = (ent.AvgLatencyMs*float64(ent.Count) + ms) / float64(ent.Count+1)
	ent.Count++
	if hits == 0 {
		ent.ZeroResults++
	}
	ent.LastSeen = time.Now().UTC()
}

func (s *VectorStore) handleSearchExtrasAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "AdvancedTextSearch":
		var req AdvancedTextQueryRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		resp, err := s.GetTextSearchManager().Search(stream.Context(), req)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(resp)
		return true, stream.Send(&flight.Result{Body: body})

	case "SearchFacets":
		var req AdvancedTextQueryRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		resp, err := s.GetTextSearchManager().Search(stream.Context(), req)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"facets": resp.Facets, "total_hits": resp.TotalHits})
		return true, stream.Send(&flight.Result{Body: body})

	case "SearchAnalytics":
		var req struct {
			Limit int `json:"limit"`
		}
		_ = json.Unmarshal(action.Body, &req)
		snap := s.GetTextSearchManager().Analytics(req.Limit)
		body, _ := json.Marshal(snap)
		return true, stream.Send(&flight.Result{Body: body})

	case "TuneBM25":
		var req struct {
			Dataset string  `json:"dataset"`
			K1      float64 `json:"k1"`
			B       float64 `json:"b"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if err := s.GetTextSearchManager().SetBM25Params(req.Dataset, BM25TuningParams{K1: req.K1, B: req.B}); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"bm25_tuned"}`)})
	}
	return false, nil
}
