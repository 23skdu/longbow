package store

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

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
