package store

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/23skdu/longbow/internal/simd"
)

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
