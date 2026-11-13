package store

import (
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

// AdvancedTextSearchManager provides full-text boolean/phrase/fuzzy search, highlighting,
// faceted counts, dynamic BM25 k1/b tuning, and search analytics tracking.
type AdvancedTextSearchManager struct {
	mu           sync.RWMutex
	store        *VectorStore
	datasetBM25  map[string]BM25TuningParams
	totalQueries int64
	zeroQueries  int64
	analytics    map[string]*QueryAnalyticsEntry
}

// NewAdvancedTextSearchManager creates an advanced text search & analytics engine.
func NewAdvancedTextSearchManager(s *VectorStore) *AdvancedTextSearchManager {
	return &AdvancedTextSearchManager{
		store:       s,
		datasetBM25: make(map[string]BM25TuningParams),
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

	type textDoc struct {
		id       string
		vectorID uint32
		text     string
		meta     map[string]any
		tokens   []string
	}
	var docs []textDoc
	if m.store != nil {
		ds, ok := m.store.getDataset(req.Dataset)
		if !ok {
			m.recordAnalytics(req.Query, 0, time.Since(start))
			return nil, fmt.Errorf("dataset %q not found", req.Dataset)
		}
		ds.dataMu.RLock()
		records := ds.Records.Read()
		tombstones := ds.Tombstones
		var vid uint32
		for bIdx, rec := range records {
			ts := tombstones[bIdx]
			colIdx := make(map[string]int, len(rec.Schema().Fields()))
			for i, f := range rec.Schema().Fields() {
				colIdx[f.Name] = i
			}
			for r := 0; r < int(rec.NumRows()); r++ {
				if ts != nil && ts.Contains(r) {
					vid++
					continue
				}
				var meta map[string]any
				if mi, hasMeta := colIdx["metadata"]; hasMeta {
					if sa, ok := rec.Column(mi).(*array.String); ok && !sa.IsNull(r) {
						_ = json.Unmarshal([]byte(sa.Value(r)), &meta)
					}
				}
				idVal := readRowStringValue(rec, colIdx, meta, r, "id")
				if idVal == "" {
					idVal = fmt.Sprintf("%d", vid)
				}
				text := collectDocumentText(rec, colIdx, meta, r)
				docs = append(docs, textDoc{
					id:       idVal,
					vectorID: vid,
					text:     text,
					meta:     meta,
					tokens:   tokenizeAlphaNum(text),
				})
				vid++
			}
		}
		ds.dataMu.RUnlock()
	}

	// Corpus statistics for dynamic BM25 (IDF + avgdl)
	nDocs := len(docs)
	var totalTokens int
	df := make(map[string]int)
	for _, d := range docs {
		totalTokens += len(d.tokens)
		seen := make(map[string]struct{}, len(d.tokens))
		for _, tok := range d.tokens {
			if _, ok := seen[tok]; !ok {
				seen[tok] = struct{}{}
				df[tok]++
			}
		}
	}
	avgDL := 1.0
	if nDocs > 0 {
		avgDL = max(1.0, float64(totalTokens)/float64(nDocs))
	}

	var hits []HighlightedTextHit
	facetMaps := make(map[string]map[string]int)
	for _, f := range req.FacetFields {
		facetMaps[f] = make(map[string]int)
	}

	for _, d := range docs {
		ok, matchedTerms := ast.evaluate(strings.ToLower(d.text), d.tokens, req.FuzzyMaxDist)
		if !ok {
			continue
		}
		score := scoreDocBM25(d.tokens, matchedTerms, nDocs, avgDL, df, bm25)
		snippet, hl := GenerateSnippetHighlight(d.text, matchedTerms, openTag, closeTag, req.SnippetChars)
		hits = append(hits, HighlightedTextHit{
			ID:         d.id,
			VectorID:   d.vectorID,
			Score:      score,
			Snippet:    snippet,
			Highlights: hl,
			Metadata:   d.meta,
		})
		for _, ff := range req.FacetFields {
			if val, exists := d.meta[ff]; exists && val != nil {
				facetMaps[ff][fmt.Sprintf("%v", val)]++
			}
		}
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].ID < hits[j].ID
		}
		return hits[i].Score > hits[j].Score
	})
	totalHits := len(hits)
	if len(hits) > req.K {
		hits = hits[:req.K]
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

func scoreDocBM25(docTokens, matchedTerms []string, nDocs int, avgDL float64, df map[string]int, params BM25TuningParams) float64 {
	if len(matchedTerms) == 0 || len(docTokens) == 0 {
		return 0
	}
	tfMap := make(map[string]int, len(docTokens))
	for _, t := range docTokens {
		tfMap[t]++
	}
	uniq := dedupStrings(matchedTerms)
	dl := float64(len(docTokens))
	var score float64
	for _, term := range uniq {
		// Split phrases into component words when scoring BM25
		for _, sub := range tokenizeAlphaNum(term) {
			tf := float64(tfMap[sub])
			if tf == 0 {
				continue
			}
			n := float64(df[sub])
			idf := math.Log(1.0 + (float64(nDocs)-n+0.5)/(n+0.5))
			num := tf * (params.K1 + 1.0)
			den := tf + params.K1*(1.0-params.B+params.B*(dl/avgDL))
			score += idf * (num / max(1e-9, den))
		}
	}
	return score
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
