package store

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BM25TuningParams configures dynamic per-dataset or per-query BM25 k1 and b parameters.
type BM25TuningParams struct {
	K1 float64 `json:"k1"`
	B  float64 `json:"b"`
}

// DefaultBM25Tuning returns standard Okapi BM25 parameters (k1=1.2, b=0.75).
func DefaultBM25Tuning() BM25TuningParams {
	return BM25TuningParams{K1: 1.2, B: 0.75}
}

// AdvancedTextQueryRequest supports boolean, phrase, fuzzy matching, highlighting, and facets.
type AdvancedTextQueryRequest struct {
	Dataset      string            `json:"dataset"`
	Query        string            `json:"query"`
	K            int               `json:"k"`
	FuzzyMaxDist int               `json:"fuzzy_max_dist,omitempty"` // optional global Levenshtein distance (default allows term~N)
	HighlightTag string            `json:"highlight_tag,omitempty"`  // default "<mark>" / "</mark>"
	SnippetChars int               `json:"snippet_chars,omitempty"`  // default 160
	FacetFields  []string          `json:"facet_fields,omitempty"`
	BM25         *BM25TuningParams `json:"bm25,omitempty"`
}

// FacetCount holds one category bucket frequency.
type FacetCount struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// HighlightedTextHit represents one full-text match with snippet highlights and metadata.
type HighlightedTextHit struct {
	ID         string         `json:"id"`
	VectorID   uint32         `json:"vector_id"`
	Score      float64        `json:"score"`
	Snippet    string         `json:"snippet"`
	Highlights []string       `json:"highlights,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// AdvancedTextSearchResponse bundles hits, facet counts, and tuned BM25 settings.
type AdvancedTextSearchResponse struct {
	Query     string                  `json:"query"`
	TotalHits int                     `json:"total_hits"`
	TookMs    float64                 `json:"took_ms"`
	Hits      []HighlightedTextHit    `json:"hits"`
	Facets    map[string][]FacetCount `json:"facets,omitempty"`
	BM25Used  BM25TuningParams        `json:"bm25_used"`
}

// QueryAnalyticsEntry summarizes query popularity and zero-result diagnostics.
type QueryAnalyticsEntry struct {
	Query        string    `json:"query"`
	Count        int64     `json:"count"`
	ZeroResults  int64     `json:"zero_results"`
	AvgLatencyMs float64   `json:"avg_latency_ms"`
	LastSeen     time.Time `json:"last_seen"`
}

// SearchAnalyticsSnapshot exposes top queries and zero-result queries.
type SearchAnalyticsSnapshot struct {
	TotalQueries      int64                 `json:"total_queries"`
	ZeroResultQueries int64                 `json:"zero_result_queries"`
	TopQueries        []QueryAnalyticsEntry `json:"top_queries"`
	ZeroResultTop     []QueryAnalyticsEntry `json:"zero_result_top"`
}

// --- Boolean + Phrase + Fuzzy AST ---

type queryNode interface {
	evaluate(docLower string, docTokens []string, defaultFuzzy int) (bool, []string)
}

type andNode struct{ left, right queryNode }
type orNode struct{ left, right queryNode }
type notNode struct{ child queryNode }
type phraseNode struct{ phrase string }
type termNode struct {
	term    string
	maxDist int
}

func (n andNode) evaluate(docLower string, docTokens []string, defF int) (bool, []string) {
	okL, mL := n.left.evaluate(docLower, docTokens, defF)
	if !okL {
		return false, nil
	}
	okR, mR := n.right.evaluate(docLower, docTokens, defF)
	if !okR {
		return false, nil
	}
	return true, append(mL, mR...)
}

func (n orNode) evaluate(docLower string, docTokens []string, defF int) (bool, []string) {
	okL, mL := n.left.evaluate(docLower, docTokens, defF)
	okR, mR := n.right.evaluate(docLower, docTokens, defF)
	if !okL && !okR {
		return false, nil
	}
	return true, append(mL, mR...)
}

func (n notNode) evaluate(docLower string, docTokens []string, defF int) (bool, []string) {
	ok, _ := n.child.evaluate(docLower, docTokens, defF)
	return !ok, nil
}

func (n phraseNode) evaluate(docLower string, _ []string, _ int) (bool, []string) {
	p := strings.ToLower(n.phrase)
	if p == "" {
		return false, nil
	}
	if strings.Contains(docLower, p) {
		return true, []string{p}
	}
	return false, nil
}

func (n termNode) evaluate(_ string, docTokens []string, defF int) (bool, []string) {
	maxDist := n.maxDist
	if maxDist < 0 {
		maxDist = defF
	}
	target := strings.ToLower(n.term)
	if target == "" {
		return false, nil
	}
	var matched []string
	seen := make(map[string]struct{})
	for _, tok := range docTokens {
		if tok == target || (maxDist > 0 && LevenshteinDistance(tok, target) <= maxDist) {
			if _, ok := seen[tok]; !ok {
				seen[tok] = struct{}{}
				matched = append(matched, tok)
			}
		}
	}
	return len(matched) > 0, matched
}

type lexToken struct {
	kind string // "WORD", "PHRASE", "AND", "OR", "NOT", "LPAREN", "RPAREN"
	val  string
}

func lexTextQuery(q string) []lexToken {
	var out []lexToken
	for i := 0; i < len(q); {
		if unicode.IsSpace(rune(q[i])) {
			i++
			continue
		}
		switch q[i] {
		case '(':
			out = append(out, lexToken{kind: "LPAREN"})
			i++
		case ')':
			out = append(out, lexToken{kind: "RPAREN"})
			i++
		case '"':
			j := i + 1
			for j < len(q) && q[j] != '"' {
				j++
			}
			out = append(out, lexToken{kind: "PHRASE", val: q[i+1 : j]})
			if j < len(q) {
				i = j + 1
			} else {
				i = j
			}
		default:
			j := i
			for j < len(q) && !unicode.IsSpace(rune(q[j])) && q[j] != '(' && q[j] != ')' && q[j] != '"' {
				j++
			}
			word := q[i:j]
			i = j
			switch strings.ToUpper(word) {
			case "AND":
				out = append(out, lexToken{kind: "AND"})
			case "OR":
				out = append(out, lexToken{kind: "OR"})
			case "NOT":
				out = append(out, lexToken{kind: "NOT"})
			default:
				if strings.HasPrefix(word, "-") && len(word) > 1 {
					out = append(out, lexToken{kind: "NOT"}, lexToken{kind: "WORD", val: word[1:]})
				} else {
					out = append(out, lexToken{kind: "WORD", val: word})
				}
			}
		}
	}
	return out
}

type queryParser struct {
	toks []lexToken
	pos  int
}

// ParseAdvancedTextQuery parses boolean (AND/OR/NOT), parentheses, "exact phrase", and fuzzy~N syntax.
func ParseAdvancedTextQuery(q string) (queryNode, error) {
	toks := lexTextQuery(q)
	if len(toks) == 0 {
		return nil, errors.New("empty text query")
	}
	p := &queryParser{toks: toks}
	node := p.parseOr()
	if node == nil {
		return nil, errors.New("could not parse query")
	}
	return node, nil
}

func (p *queryParser) peek() (lexToken, bool) {
	if p.pos >= len(p.toks) {
		return lexToken{}, false
	}
	return p.toks[p.pos], true
}

func (p *queryParser) parseOr() queryNode {
	left := p.parseAnd()
	for {
		tok, ok := p.peek()
		if ok && tok.kind == "OR" {
			p.pos++
			right := p.parseAnd()
			if right != nil {
				left = orNode{left: left, right: right}
			}
			continue
		}
		break
	}
	return left
}

func (p *queryParser) parseAnd() queryNode {
	left := p.parseUnary()
	for {
		tok, ok := p.peek()
		if !ok || tok.kind == "OR" || tok.kind == "RPAREN" {
			break
		}
		if tok.kind == "AND" {
			p.pos++
			right := p.parseUnary()
			if right != nil {
				left = andNode{left: left, right: right}
			}
			continue
		}
		// Implicit OR between bare adjacent terms
		right := p.parseUnary()
		if right == nil {
			break
		}
		left = orNode{left: left, right: right}
	}
	return left
}

func (p *queryParser) parseUnary() queryNode {
	tok, ok := p.peek()
	if !ok {
		return nil
	}
	if tok.kind == "NOT" {
		p.pos++
		child := p.parseUnary()
		if child == nil {
			return nil
		}
		return notNode{child: child}
	}
	if tok.kind == "LPAREN" {
		p.pos++
		inner := p.parseOr()
		if next, ok := p.peek(); ok && next.kind == "RPAREN" {
			p.pos++
		}
		return inner
	}
	if tok.kind == "PHRASE" {
		p.pos++
		return phraseNode{phrase: tok.val}
	}
	if tok.kind == "WORD" {
		p.pos++
		term, maxDist := parseFuzzySuffix(tok.val)
		return termNode{term: term, maxDist: maxDist}
	}
	p.pos++
	return nil
}

func parseFuzzySuffix(word string) (string, int) {
	if idx := strings.LastIndexByte(word, '~'); idx > 0 {
		term := word[:idx]
		distStr := word[idx+1:]
		if distStr == "" {
			return term, 1
		}
		if d, err := strconv.Atoi(distStr); err == nil && d >= 0 && d <= 4 {
			return term, d
		}
	}
	return word, -1
}

// LevenshteinDistance computes Optimal String Alignment (Damerau-Levenshtein) edit distance
// between two UTF-8 strings, counting adjacent character transpositions as distance 1.
func LevenshteinDistance(a, b string) int {
	ra := []rune(a)
	rb := []rune(b)
	la, lb := len(ra), len(rb)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prevPrev := make([]int, lb+1)
	prev := make([]int, lb+1)
	cur := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		cur[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := cur[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			if i > 1 && j > 1 && ra[i-1] == rb[j-2] && ra[i-2] == rb[j-1] {
				trans := prevPrev[j-2] + cost
				if trans < m {
					m = trans
				}
			}
			cur[j] = m
		}
		prevPrev, prev, cur = prev, cur, prevPrev
	}
	return prev[lb]
}

// GenerateSnippetHighlight wraps matched tokens/phrases in tags (e.g. <mark>..</mark>)
// and extracts a compact contextual snippet centered around the first match.
func GenerateSnippetHighlight(text string, matchedTerms []string, openTag, closeTag string, maxLen int) (string, []string) {
	if text == "" {
		return "", nil
	}
	if openTag == "" {
		openTag = "<mark>"
	}
	if closeTag == "" {
		closeTag = "</mark>"
	}
	if maxLen <= 0 {
		maxLen = 160
	}

	lower := strings.ToLower(text)
	firstMatch := -1
	var highlights []string

	// Sort longest matches first so phrases take precedence over sub-terms
	uniq := dedupStrings(matchedTerms)
	sort.Slice(uniq, func(i, j int) bool { return len(uniq[i]) > len(uniq[j]) })

	var spans []highlightSpan
	for _, term := range uniq {
		lt := strings.ToLower(term)
		if lt == "" {
			continue
		}
		offset := 0
		for {
			idx := strings.Index(lower[offset:], lt)
			if idx < 0 {
				break
			}
			absStart := offset + idx
			absEnd := absStart + len(lt)
			if !overlapsSpan(spans, absStart, absEnd) {
				spans = append(spans, highlightSpan{start: absStart, end: absEnd})
				if firstMatch < 0 || absStart < firstMatch {
					firstMatch = absStart
				}
			}
			offset = absEnd
		}
	}

	if len(spans) == 0 {
		if len(text) > maxLen {
			return text[:maxLen] + "...", nil
		}
		return text, nil
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })

	winStart := 0
	winEnd := len(text)
	if len(text) > maxLen {
		center := firstMatch
		winStart = max(0, center-maxLen/3)
		winEnd = min(len(text), winStart+maxLen)
		if winStart > 0 {
			for winStart < len(text) && text[winStart] != ' ' && winStart-max(0, center-maxLen/3) < 16 {
				winStart++
			}
		}
	}

	var out strings.Builder
	if winStart > 0 {
		out.WriteString("...")
	}
	cursor := winStart
	for _, sp := range spans {
		if sp.end <= winStart || sp.start >= winEnd {
			continue
		}
		s := max(sp.start, winStart)
		e := min(sp.end, winEnd)
		out.WriteString(text[cursor:s])
		frag := text[s:e]
		highlights = append(highlights, openTag+frag+closeTag)
		out.WriteString(openTag)
		out.WriteString(frag)
		out.WriteString(closeTag)
		cursor = e
	}
	if cursor < winEnd {
		out.WriteString(text[cursor:winEnd])
	}
	if winEnd < len(text) {
		out.WriteString("...")
	}
	return out.String(), highlights
}

type cachedTextDoc struct {
	id        string
	vectorID  uint32
	text      string
	lowerText string
	meta      map[string]any
	tokens    []string
}

// textShardSignature identifies one record batch's contribution to the corpus.
// A shard may be reused only while its batch has the same shape and tombstone
// set. Row counts matter beyond the shard itself: they determine every later
// shard's vector IDs, so a change in one batch invalidates all later shards.
type textShardSignature struct {
	rows          int64
	tombstoneCard uint64
	recordID      string
	tombstoneID   string
}

// cachedTextShard is the corpus for one record batch. Shards are immutable once
// built, so they can be reused across queries and across corpus rebuilds.
type cachedTextShard struct {
	sig         textShardSignature
	docs        []cachedTextDoc
	df          map[string]int
	postings    map[string][]int32
	trigrams    *index.TrigramIndex
	totalTokens int
}

// cachedTextCorpus is a sharded, immutable view of a dataset's text.
//
// The corpus is rebuilt incrementally: only batches whose signature changed are
// re-tokenised. Global BM25 inputs (document count, average length and document
// frequency) are merged across shards so scoring is identical to a flat corpus.
type cachedTextCorpus struct {
	numBatches    int
	totalRows     int64
	tombstoneCard uint64
	shards        []*cachedTextShard
	docBase       []int // len(shards)+1 prefix sums of live document counts
	df            map[string]int
	vocab         []string
	vocabByLen    map[int][]string
	nDocs         int
	avgDL         float64

	fuzzyMu    sync.RWMutex
	fuzzyCache map[string][]string
}

// docAt resolves a global document index to its shard and document.
func (c *cachedTextCorpus) docAt(idx int) *cachedTextDoc {
	if idx < 0 || len(c.shards) == 0 {
		return nil
	}
	shard := sort.Search(len(c.shards), func(i int) bool { return c.docBase[i+1] > idx })
	if shard >= len(c.shards) || idx < c.docBase[shard] {
		return nil
	}
	local := idx - c.docBase[shard]
	if local < 0 || local >= len(c.shards[shard].docs) {
		return nil
	}
	return &c.shards[shard].docs[local]
}

// textCandidate is a scored document candidate for the top-K heap.
type textCandidate struct {
	docIdx int
	score  float64
}

// textCandidateHeap is a min-heap by score for top-K selection.
type textCandidateHeap struct {
	items []textCandidate
	k     int
}

func (h *textCandidateHeap) Len() int { return len(h.items) }
func (h *textCandidateHeap) Less(i, j int) bool {
	if h.items[i].score == h.items[j].score {
		// The root is the worst retained candidate. On a score tie keep the
		// lower document index so repeated queries return the same top-K.
		return h.items[i].docIdx > h.items[j].docIdx
	}
	return h.items[i].score < h.items[j].score
}
func (h *textCandidateHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *textCandidateHeap) Push(x interface{}) {
	h.items = append(h.items, x.(textCandidate))
}
func (h *textCandidateHeap) Pop() interface{} {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

// push maintains the top-K: evict the smallest when capacity exceeded.
func (h *textCandidateHeap) pushTop(c textCandidate) {
	if h.k <= 0 || len(h.items) < h.k {
		heap.Push(h, c)
		return
	}
	if c.score > h.items[0].score {
		heap.Pop(h)
		heap.Push(h, c)
	}
}

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
func (m *AdvancedTextSearchManager) corpusFor(dataset string) (*cachedTextCorpus, error) {
	ds, ok := m.store.getDataset(dataset)
	if !ok {
		return nil, fmt.Errorf("dataset %q not found", dataset)
	}

	ds.dataMu.RLock()
	records := ds.Records.Read()
	tombstones := ds.Tombstones

	m.mu.RLock()
	prev := m.corpusCache[dataset]
	m.mu.RUnlock()

	corpus, changed := buildTextCorpus(records, tombstones, prev)
	ds.dataMu.RUnlock()

	if changed {
		m.mu.Lock()
		m.corpusCache[dataset] = corpus
		m.mu.Unlock()
	}
	return corpus, nil
}

// buildTextCorpus assembles a sharded corpus, reusing unchanged shards from
// prev. A change in one batch invalidates every later shard because vector IDs
// are global row numbers, so later shards are rebuilt even when their own
// signature still matches.
func buildTextCorpus(records []arrow.RecordBatch, tombstones map[int]*types.Bitset, prev *cachedTextCorpus) (*cachedTextCorpus, bool) {
	// A shorter batch list invalidates index alignment, so rebuild everything.
	invalidStructure := prev == nil || len(prev.shards) > len(records)
	changed := invalidStructure
	baseDirty := false
	shards := make([]*cachedTextShard, len(records))
	rowBase := 0
	for i, rec := range records {
		ts := tombstones[i]
		sig := textShardSignature{
			rows:          rec.NumRows(),
			tombstoneCard: textTombstoneCount(ts),
			recordID:      textObjectIdentity(rec),
			tombstoneID:   textObjectIdentity(ts),
		}
		canReuse := prev != nil && !invalidStructure && !baseDirty && i < len(prev.shards) && prev.shards[i].sig == sig
		if canReuse {
			shards[i] = prev.shards[i]
		} else {
			changed = true
			shards[i] = buildTextShard(rec, ts, rowBase)
		}
		// Vector IDs are global row numbers. If this batch's row count changed,
		// every later shard's base moved and must be rebuilt even when its own
		// signature still matches.
		if prev != nil && i < len(prev.shards) && prev.shards[i].sig.rows != rec.NumRows() {
			baseDirty = true
		}
		rowBase += int(rec.NumRows())
	}

	if !changed {
		return prev, false
	}

	corpus := &cachedTextCorpus{
		numBatches:    len(records),
		totalRows:     int64(rowBase),
		tombstoneCard: 0,
		shards:        shards,
		docBase:       make([]int, len(shards)+1),
		fuzzyCache:    make(map[string][]string),
	}
	for _, sh := range shards {
		corpus.tombstoneCard += sh.sig.tombstoneCard
		corpus.nDocs += len(sh.docs)
	}
	for i, sh := range shards {
		corpus.docBase[i+1] = corpus.docBase[i] + len(sh.docs)
	}

	var totalTokens int
	for _, sh := range shards {
		totalTokens += sh.totalTokens
	}
	corpus.avgDL = 1.0
	if corpus.nDocs > 0 {
		corpus.avgDL = max(1.0, float64(totalTokens)/float64(corpus.nDocs))
	}

	// Fold document frequencies incrementally: subtract the shards that were
	// replaced and add the new ones. Unchanged shards cancel out, so appending a
	// batch does not re-merge the whole vocabulary.
	if prev != nil && len(prev.df) > 0 {
		corpus.df = make(map[string]int, len(prev.df))
		for term, n := range prev.df {
			corpus.df[term] = n
		}
		for i, old := range prev.shards {
			if i < len(shards) && shards[i] == old {
				continue
			}
			for term, n := range old.df {
				corpus.df[term] -= n
				if corpus.df[term] <= 0 {
					delete(corpus.df, term)
				}
			}
		}
		for i, sh := range shards {
			if i < len(prev.shards) && prev.shards[i] == sh {
				continue
			}
			for term, n := range sh.df {
				corpus.df[term] += n
			}
		}
	} else {
		corpus.df = make(map[string]int)
		for _, sh := range shards {
			for term, n := range sh.df {
				corpus.df[term] += n
			}
		}
	}

	corpus.vocab = make([]string, 0, len(corpus.df))
	corpus.vocabByLen = make(map[int][]string)
	for term := range corpus.df {
		corpus.vocab = append(corpus.vocab, term)
		corpus.vocabByLen[len(term)] = append(corpus.vocabByLen[len(term)], term)
	}
	return corpus, true
}

// buildTextShard tokenises one record batch. rowBase is the number of rows in
// all earlier batches, so vector IDs stay global row numbers.
func buildTextShard(rec arrow.RecordBatch, tombstones *types.Bitset, rowBase int) *cachedTextShard {
	fields := rec.Schema().Fields()
	colIdx := make(map[string]int, len(fields))
	for i, f := range fields {
		colIdx[f.Name] = i
	}

	rows := int(rec.NumRows())
	docs := make([]cachedTextDoc, 0, rows)
	df := make(map[string]int)
	postings := make(map[string][]int32)
	trigrams := index.NewTrigramIndex()
	totalTokens := 0

	for r := 0; r < rows; r++ {
		if tombstones != nil && tombstones.Contains(r) {
			continue
		}
		var meta map[string]any
		if mi, hasMeta := colIdx["metadata"]; hasMeta {
			if sa, ok := rec.Column(mi).(*array.String); ok && !sa.IsNull(r) {
				_ = json.Unmarshal([]byte(sa.Value(r)), &meta)
			}
		}
		vectorID := uint32(rowBase + r) // #nosec G115 -- row counts fit the corpus address space
		idVal := readRowStringValue(rec, colIdx, meta, r, "id")
		if idVal == "" {
			idVal = strconv.FormatUint(uint64(vectorID), 10)
		}
		text := collectDocumentText(rec, colIdx, meta, r)
		lowerText := strings.ToLower(text)
		tokens := tokenizeAlphaNum(text)
		docPos := int32(len(docs)) // #nosec G115 -- a shard is one Arrow batch
		indexShardTokens(tokens, docPos, df, postings)
		totalTokens += len(tokens)
		trigrams.IndexString(docPos, lowerText)
		docs = append(docs, cachedTextDoc{
			id:        idVal,
			vectorID:  vectorID,
			text:      text,
			lowerText: lowerText,
			meta:      meta,
			tokens:    tokens,
		})
	}

	return &cachedTextShard{
		sig: textShardSignature{
			rows:          rec.NumRows(),
			tombstoneCard: textTombstoneCount(tombstones),
			recordID:      textObjectIdentity(rec),
			tombstoneID:   textObjectIdentity(tombstones),
		},
		docs:        docs,
		df:          df,
		postings:    postings,
		trigrams:    trigrams,
		totalTokens: totalTokens,
	}
}

// indexShardTokens records first-occurrence document frequency and postings for
// one document. Small documents use a linear duplicate check; long ones use a
// set so indexing stays O(tokens).
func indexShardTokens(tokens []string, docPos int32, df map[string]int, postings map[string][]int32) {
	if len(tokens) > 16 {
		seen := make(map[string]struct{}, len(tokens))
		for _, tok := range tokens {
			if _, ok := seen[tok]; ok {
				continue
			}
			seen[tok] = struct{}{}
			df[tok]++
			postings[tok] = append(postings[tok], docPos)
		}
		return
	}
	for i, tok := range tokens {
		dup := false
		for j := 0; j < i; j++ {
			if tokens[j] == tok {
				dup = true
				break
			}
		}
		if !dup {
			df[tok]++
			postings[tok] = append(postings[tok], docPos)
		}
	}
}

func textObjectIdentity(v any) string {
	return fmt.Sprintf("%p", v)
}

func textTombstoneCount(ts *types.Bitset) uint64 {
	if ts == nil {
		return 0
	}
	return ts.Count()
}

// Analytics returns search volume, top queries, and zero-result queries.
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

// textScoreCandidate is one document that satisfied the query AST, held until
// the whole shard (or block) can be scored in a batch.
type textScoreCandidate struct {
	docIdx  int
	tokens  []string
	matched []string
}

// evaluateTextCorpus walks shards, narrows each to candidates, evaluates the AST
// and feeds the ranked hits into th. It returns the number of matching documents
// (not just the retained top-K).
func evaluateTextCorpus(corpus *cachedTextCorpus, ast queryNode, req AdvancedTextQueryRequest, bm25 BM25TuningParams, th *textCandidateHeap, facets map[string]map[string]int) int {
	if corpus == nil || ast == nil {
		return 0
	}
	matchedTotal := 0
	for shardIdx, shard := range corpus.shards {
		if len(shard.docs) == 0 {
			continue
		}
		cands := collectShardCandidates(ast, corpus, shard, req.FuzzyMaxDist)
		base := corpus.docBase[shardIdx]
		capacity := len(shard.docs)
		if !cands.all {
			capacity = len(cands.idxs)
		}
		batch := make([]textScoreCandidate, 0, capacity)
		visit := func(local int32) {
			if local < 0 || int(local) >= len(shard.docs) {
				return
			}
			d := &shard.docs[local]
			ok, matchedTerms := ast.evaluate(d.lowerText, d.tokens, req.FuzzyMaxDist)
			if !ok {
				return
			}
			matchedTotal++
			batch = append(batch, textScoreCandidate{
				docIdx:  base + int(local),
				tokens:  d.tokens,
				matched: matchedTerms,
			})
			for _, ff := range req.FacetFields {
				if val, exists := d.meta[ff]; exists && val != nil {
					facets[ff][fmt.Sprintf("%v", val)]++
				}
			}
		}
		if cands.all {
			for i := range shard.docs {
				visit(int32(i)) // #nosec G115 -- a shard is one Arrow batch
			}
		} else {
			for _, local := range cands.idxs {
				visit(local)
			}
		}
		if len(batch) == 0 {
			continue
		}
		scores := scoreTextCandidates(batch, corpus.nDocs, corpus.avgDL, corpus.df, bm25)
		for i := range batch {
			th.pushTop(textCandidate{docIdx: batch[i].docIdx, score: scores[i]})
		}
	}
	return matchedTotal
}

// shardCandidateSet is a candidate list within one shard. all means "every live
// document in this shard"; idxs is sorted ascending and read-only.
type shardCandidateSet struct {
	idxs []int32
	all  bool
}

// collectShardCandidates narrows a query to documents that can possibly match.
// Every branch is a superset: phrase trigrams may admit non-contiguous matches
// and NOT is represented by "all", so evaluate still makes the final decision.
func collectShardCandidates(node queryNode, corpus *cachedTextCorpus, shard *cachedTextShard, defFuzzy int) shardCandidateSet {
	switch n := node.(type) {
	case termNode:
		dist := n.maxDist
		if dist < 0 {
			dist = defFuzzy
		}
		target := strings.ToLower(n.term)
		if target == "" {
			return shardCandidateSet{}
		}
		terms := corpus.matchingTerms(target, dist)
		if len(terms) == 0 {
			return shardCandidateSet{}
		}
		if len(terms) == 1 {
			return shardCandidateSet{idxs: shard.postings[terms[0]]}
		}
		var out []int32
		for _, term := range terms {
			out = unionSortedInt32(out, shard.postings[term])
		}
		return shardCandidateSet{idxs: out}
	case phraseNode:
		p := strings.ToLower(n.phrase)
		if p == "" {
			return shardCandidateSet{}
		}
		// Lookup cannot bound values shorter than one trigram; evaluate them
		// against the whole shard rather than dropping real matches.
		if len(p) < 3 {
			return shardCandidateSet{all: true}
		}
		return shardCandidateSet{idxs: shard.trigrams.Lookup(p)}
	case andNode:
		left := collectShardCandidates(n.left, corpus, shard, defFuzzy)
		right := collectShardCandidates(n.right, corpus, shard, defFuzzy)
		switch {
		case left.all && right.all:
			return shardCandidateSet{all: true}
		case left.all:
			return right
		case right.all:
			return left
		default:
			return shardCandidateSet{idxs: intersectSortedInt32(left.idxs, right.idxs)}
		}
	case orNode:
		left := collectShardCandidates(n.left, corpus, shard, defFuzzy)
		right := collectShardCandidates(n.right, corpus, shard, defFuzzy)
		if left.all || right.all {
			return shardCandidateSet{all: true}
		}
		return shardCandidateSet{idxs: unionSortedInt32(left.idxs, right.idxs)}
	case notNode:
		return shardCandidateSet{all: true}
	default:
		return shardCandidateSet{all: true}
	}
}

// matchingTerms returns the vocabulary terms that termNode would accept for
// target at edit distance dist. Exact terms are a single posting lookup; fuzzy
// terms scan only vocabulary entries whose length can be within dist.
func (c *cachedTextCorpus) matchingTerms(target string, dist int) []string {
	key := target + "\x00" + strconv.Itoa(dist)
	c.fuzzyMu.RLock()
	if cached, ok := c.fuzzyCache[key]; ok {
		c.fuzzyMu.RUnlock()
		return cached
	}
	c.fuzzyMu.RUnlock()

	var out []string
	if dist <= 0 {
		if _, ok := c.df[target]; ok {
			out = []string{target}
		}
	} else if isASCII(target) {
		targetLen := len(target)
		size := targetLen + dist + 1
		prevPrev := make([]int, size)
		prev := make([]int, size)
		cur := make([]int, size)
		for length := max(1, targetLen-dist); length <= targetLen+dist; length++ {
			for _, term := range c.vocabByLen[length] {
				if term == target || osaWithin(target, term, dist, prevPrev[:length+1], prev[:length+1], cur[:length+1]) {
					out = append(out, term)
				}
			}
		}
	} else {
		for _, term := range c.vocab {
			if intAbs(len(term)-len(target)) > dist {
				continue
			}
			if term == target || LevenshteinDistance(term, target) <= dist {
				out = append(out, term)
			}
		}
	}

	c.fuzzyMu.Lock()
	c.fuzzyCache[key] = out
	c.fuzzyMu.Unlock()
	return out
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func intAbs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

// osaWithin is LevenshteinDistance with optimal-string-alignment transpositions
// and an early cutoff. It uses caller-provided rows so a vocabulary scan does
// not allocate per candidate. Inputs and rows are byte-indexed and therefore
// only valid for ASCII, which is all tokenizeAlphaNum can produce.
func osaWithin(a, b string, maxDist int, prevPrev, prev, cur []int) bool {
	la, lb := len(a), len(b)
	if intAbs(la-lb) > maxDist {
		return false
	}
	if la == 0 {
		return lb <= maxDist
	}
	if lb == 0 {
		return la <= maxDist
	}
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		cur[0] = i
		rowMin := cur[0]
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			v := min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
			if i > 1 && j > 1 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				v = min(v, prevPrev[j-2]+1)
			}
			cur[j] = v
			if v < rowMin {
				rowMin = v
			}
		}
		if rowMin > maxDist {
			return false
		}
		prevPrev, prev, cur = prev, cur, prevPrev
	}
	return prev[lb] <= maxDist
}

func intersectSortedInt32(a, b []int32) []int32 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	// When one side is much longer, binary-searching it for each element of the
	// short side beats walking both. This is the common shape of an AND between
	// a stopword-like term and a selective one.
	if len(a) > 8*len(b) {
		return intersectSortedInt32BySearch(a, b)
	}
	if len(b) > 8*len(a) {
		return intersectSortedInt32BySearch(b, a)
	}
	out := make([]int32, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

func intersectSortedInt32BySearch(long, short []int32) []int32 {
	out := make([]int32, 0, len(short))
	for _, v := range short {
		i := sort.Search(len(long), func(i int) bool { return long[i] >= v })
		if i < len(long) && long[i] == v {
			out = append(out, v)
		}
	}
	return out
}

func unionSortedInt32(a, b []int32) []int32 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]int32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// scoreCachedDocBM25 scores one document from its tokens. Repeated sub-terms
// across a boolean query are counted once, which keeps scalar and SIMD scoring
// on the same canonical term set.
func scoreCachedDocBM25(tokens []string, matchedTerms []string, nDocs int, avgDL float64, df map[string]int, params BM25TuningParams) float64 {
	if len(matchedTerms) == 0 || len(tokens) == 0 {
		return 0
	}
	var stack [16]string
	subs := stack[:0]
	for _, term := range matchedTerms {
		subs = appendTermSubtokens(subs, term)
	}
	if len(subs) == 0 {
		return 0
	}
	dl := float64(len(tokens))
	var score float64
	for _, sub := range subs {
		tf := float64(docTermFrequency(tokens, sub))
		if tf == 0 {
			continue
		}
		n := float64(df[sub])
		idf := math.Log(1.0 + (float64(nDocs)-n+0.5)/(n+0.5))
		num := tf * (params.K1 + 1.0)
		den := tf + params.K1*(1.0-params.B+params.B*(dl/avgDL))
		score += idf * (num / max(1e-9, den))
	}
	return score
}

func appendTermSubtokens(dst []string, term string) []string {
	if term == "" {
		return dst
	}
	if isSimpleToken(term) {
		if !containsString(dst, term) {
			dst = append(dst, term)
		}
		return dst
	}
	for _, sub := range tokenizeAlphaNum(term) {
		if !containsString(dst, sub) {
			dst = append(dst, sub)
		}
	}
	return dst
}

func isSimpleToken(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

func containsString(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

func docTermFrequency(tokens []string, term string) int {
	n := 0
	for _, tok := range tokens {
		if tok == term {
			n++
		}
	}
	return n
}

func prepareSubTerms(matched []string) []string {
	if len(matched) == 0 {
		return nil
	}
	// The common case is a term or boolean query whose matched terms are
	// already distinct tokens. Return the evaluator's slice instead of copying
	// it once per candidate.
	allSimple := true
	for i, term := range matched {
		if !isSimpleToken(term) {
			allSimple = false
			break
		}
		for j := 0; j < i; j++ {
			if matched[j] == term {
				allSimple = false
				break
			}
		}
		if !allSimple {
			break
		}
	}
	if allSimple {
		return matched
	}
	var stack [16]string
	subs := stack[:0]
	for _, term := range matched {
		subs = appendTermSubtokens(subs, term)
	}
	if len(subs) == 0 {
		return nil
	}
	out := make([]string, len(subs))
	copy(out, subs)
	return out
}

// scoreTextCandidates scores every candidate, using the SIMD BM25 kernels when
// there is enough work to amortise the transposition. The scalar path and the
// batch path score the same canonical sub-term set, so they agree up to the
// kernels' float32 precision.
func scoreTextCandidates(cands []textScoreCandidate, nDocs int, avgDL float64, df map[string]int, params BM25TuningParams) []float64 {
	out := make([]float64, len(cands))
	if len(cands) == 0 {
		return out
	}

	const (
		simdMinCandidates = 32
		simdMaxCandidates = 100_000
		simdChunk         = 1024
		simdMinWork       = 512
		simdMaxTerms      = 64
	)
	if len(cands) < simdMinCandidates || len(cands) > simdMaxCandidates {
		for i := range cands {
			out[i] = scoreCachedDocBM25(cands[i].tokens, cands[i].matched, nDocs, avgDL, df, params)
		}
		return out
	}

	avgDL32 := float32(avgDL)
	k1 := float32(params.K1)
	b := float32(params.B)
	for start := 0; start < len(cands); start += simdChunk {
		end := min(start+simdChunk, len(cands))
		n := end - start
		subs := make([][]string, n)
		union := make(map[string]struct{})
		for i := start; i < end; i++ {
			subs[i-start] = prepareSubTerms(cands[i].matched)
			for _, term := range subs[i-start] {
				union[term] = struct{}{}
			}
		}
		if len(union) == 0 {
			continue
		}
		if len(union) > simdMaxTerms || n*len(union) < simdMinWork {
			for i := start; i < end; i++ {
				out[i] = scoreCachedDocBM25(cands[i].tokens, cands[i].matched, nDocs, avgDL, df, params)
			}
			continue
		}

		acc := make([]float32, n)
		tfs := make([]int, n)
		lens := make([]int, n)
		for i := start; i < end; i++ {
			lens[i-start] = len(cands[i].tokens)
		}
		for term := range union {
			for i := start; i < end; i++ {
				tfs[i-start] = 0
				if containsString(subs[i-start], term) {
					tfs[i-start] = docTermFrequency(cands[i].tokens, term)
				}
			}
			nTerm := float64(df[term])
			idf := math.Log(1.0 + (float64(nDocs)-nTerm+0.5)/(nTerm+0.5))
			part := simd.BM25ScoreBatch(tfs, lens, avgDL32, float32(idf), k1, b)
			for j, v := range part {
				acc[j] += v
			}
		}
		for i := start; i < end; i++ {
			out[i] = float64(acc[i-start])
		}
	}
	return out
}

func collectDocumentText(rec arrow.RecordBatch, colIdx map[string]int, meta map[string]any, row int) string {
	var parts []string
	for _, colName := range []string{"text", "title", "caption", "content", "description"} {
		if idx, ok := colIdx[colName]; ok {
			if sa, ok := rec.Column(idx).(*array.String); ok && !sa.IsNull(row) && sa.Value(row) != "" {
				parts = append(parts, sa.Value(row))
			}
		} else if meta != nil {
			if s, ok := meta[colName].(string); ok && s != "" {
				parts = append(parts, s)
			}
		}
	}
	return strings.Join(parts, " ")
}

func dedupStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

type highlightSpan struct {
	start, end int
}

func overlapsSpan(spans []highlightSpan, s, e int) bool {
	for _, sp := range spans {
		if s < sp.end && e > sp.start {
			return true
		}
	}
	return false
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
