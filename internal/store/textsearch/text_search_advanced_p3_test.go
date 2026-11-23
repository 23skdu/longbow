package textsearch

// P3 text tests: candidate generation parity against an exhaustive reference,
// scalar/SIMD BM25 parity, shard reuse, and a throughput guard at 1M documents.

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildP3TextBatch builds a minimal Arrow batch with the id and text columns
// that collectDocumentText understands.
func buildP3TextBatch(offset, rows int, textFn func(int) string) arrow.RecordBatch {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "text", Type: arrow.BinaryTypes.String},
	}, nil)
	ids := array.NewStringBuilder(pool)
	texts := array.NewStringBuilder(pool)
	defer ids.Release()
	defer texts.Release()
	for i := 0; i < rows; i++ {
		ids.Append(strconv.Itoa(offset + i))
		texts.Append(textFn(offset + i))
	}
	return array.NewRecordBatch(schema, []arrow.Array{ids.NewArray(), texts.NewArray()}, int64(rows))
}

func buildP3TextCorpus(tb testing.TB, n, batch int, textFn func(int) string) (*cachedTextCorpus, []arrow.RecordBatch) {
	tb.Helper()
	var records []arrow.RecordBatch
	for off := 0; off < n; off += batch {
		rows := min(batch, n-off)
		records = append(records, buildP3TextBatch(off, rows, textFn))
	}
	corpus, changed := buildTextCorpus(records, nil, nil)
	if !changed || corpus == nil {
		tb.Fatalf("buildTextCorpus did not build a corpus")
	}
	return corpus, records
}

func releaseP3Records(records []arrow.RecordBatch) {
	for _, rec := range records {
		rec.Release()
	}
}

type p3SearchResult struct {
	ids    []string
	scores []float64
	total  int
}

func searchP3Corpus(tb testing.TB, corpus *cachedTextCorpus, query string, k, fuzzy int) p3SearchResult {
	tb.Helper()
	ast, err := ParseAdvancedTextQuery(query)
	if err != nil {
		tb.Fatalf("parse %q: %v", query, err)
	}
	req := AdvancedTextQueryRequest{Dataset: "p3", Query: query, K: k, FuzzyMaxDist: fuzzy}
	th := &textCandidateHeap{k: k}
	facets := map[string]map[string]int{}
	total := evaluateTextCorpus(corpus, ast, req, DefaultBM25Tuning(), th, facets)
	heap.Init(th)
	sorted := make([]textCandidate, th.Len())
	for i := th.Len() - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(th).(textCandidate)
	}
	out := p3SearchResult{total: total, ids: make([]string, len(sorted)), scores: make([]float64, len(sorted))}
	for i, c := range sorted {
		d := corpus.docAt(c.docIdx)
		if d == nil {
			tb.Fatalf("candidate %d resolved to nil document", c.docIdx)
		}
		out.ids[i] = d.id
		out.scores[i] = c.score
	}
	return out
}

// bruteForceP3Text enumerates every live document, with no candidate pruning.
func bruteForceP3Text(corpus *cachedTextCorpus, query string, k, fuzzy int) p3SearchResult {
	ast, err := ParseAdvancedTextQuery(query)
	if err != nil {
		panic(err)
	}
	type hit struct {
		idx   int
		score float64
	}
	var hits []hit
	for shardIdx, shard := range corpus.shards {
		for i := range shard.docs {
			d := &shard.docs[i]
			ok, matched := ast.evaluate(d.lowerText, d.tokens, fuzzy)
			if !ok {
				continue
			}
			hits = append(hits, hit{
				idx:   corpus.docBase[shardIdx] + i,
				score: scoreCachedDocBM25(d.tokens, matched, corpus.nDocs, corpus.avgDL, corpus.df, DefaultBM25Tuning()),
			})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score == hits[j].score {
			return hits[i].idx < hits[j].idx
		}
		return hits[i].score > hits[j].score
	})
	out := p3SearchResult{total: len(hits)}
	if len(hits) > k {
		hits = hits[:k]
	}
	out.ids = make([]string, len(hits))
	out.scores = make([]float64, len(hits))
	for i, h := range hits {
		out.ids[i] = corpus.docAt(h.idx).id
		out.scores[i] = h.score
	}
	return out
}

func parityText(i int) string {
	switch i % 8 {
	case 0:
		return "the quick brown fox jumps over"
	case 1:
		return "quickly quacking ducks quack"
	case 2:
		return "brownian motion in physics theory"
	case 3:
		return "a quick brown dog barks"
	case 4:
		return "electronics sku42 red widget"
	case 5:
		return "clothing sku43 blue jacket"
	case 6:
		return "electronics sku42 blue widget"
	default:
		return "encryption encrypt encrypted decryption"
	}
}

func requireP3Parity(t *testing.T, corpus *cachedTextCorpus, query string, k, fuzzy int) {
	t.Helper()
	got := searchP3Corpus(t, corpus, query, k, fuzzy)
	want := bruteForceP3Text(corpus, query, k, fuzzy)
	if got.total != want.total {
		t.Fatalf("%q: total=%d want %d", query, got.total, want.total)
	}
	if len(got.ids) != len(want.ids) {
		t.Fatalf("%q: ids=%v want %v", query, got.ids, want.ids)
	}
	for i := range got.ids {
		if got.ids[i] != want.ids[i] {
			t.Fatalf("%q: rank %d id=%s want %s", query, i, got.ids[i], want.ids[i])
		}
		diff := math.Abs(got.scores[i] - want.scores[i])
		tol := 1e-4*math.Abs(want.scores[i]) + 1e-6
		if diff > tol {
			t.Fatalf("%q: rank %d score=%v want %v (tol %v)", query, i, got.scores[i], want.scores[i], tol)
		}
	}
}

// TestAdvancedTextCandidateParity runs the test advanced text candidate parity operation.
func TestAdvancedTextCandidateParity(t *testing.T) {
	const n = 400
	corpus, records := buildP3TextCorpus(t, n, 50, parityText)
	defer releaseP3Records(records)

	queries := []string{
		"brown",                            // exact posting list
		`"quick brown"`,                    // phrase, trigram candidates
		`"ectron"`,                         // phrase substring not token-aligned
		`"ct"`,                             // phrase shorter than a trigram
		"quack~3",                          // fuzzy vocabulary expansion
		"encrypt~2",                        // fuzzy expansion, deduped sub-terms
		"electronics AND sku42",            // selective AND
		"electronics OR clothing",          // broad OR
		"brown AND NOT quick",              // NOT forces a full shard scan
		"(quick OR brown) AND NOT physics", // nested boolean
		`"vector database" OR electronics`, // no hits on one phrase
		"sku42 AND widget",                 // phrase-free multi-term AND
		"nonexistenttermxyz",               // exact miss must be O(1), not a scan
		`"zzzzzz"`,                         // phrase miss
	}
	for _, query := range queries {
		requireP3Parity(t, corpus, query, 10, 0)
	}

	// Request-level fuzzy distance applies when the term has no ~N suffix.
	requireP3Parity(t, corpus, "quack", 10, 3)
}

// TestAdvancedTextPhraseDoesNotMissSubstrings runs the test advanced text phrase does not miss substrings operation.
func TestAdvancedTextPhraseDoesNotMissSubstrings(t *testing.T) {
	corpus, records := buildP3TextCorpus(t, 40, 10, func(i int) string {
		if i == 7 {
			return "supercalifragilistic"
		}
		return "ordinary text"
	})
	defer releaseP3Records(records)

	// "califrag" is not token-aligned: a token-posting implementation returns
	// nothing here, while phrase evaluation uses strings.Contains.
	got := searchP3Corpus(t, corpus, `"califrag"`, 5, 0)
	if got.total != 1 || len(got.ids) != 1 || got.ids[0] != "7" {
		t.Fatalf(`phrase "califrag" = %v (total %d), want id 7`, got.ids, got.total)
	}
}

// TestAdvancedTextBM25BatchParity runs the test advanced text bm25 batch parity operation.
func TestAdvancedTextBM25BatchParity(t *testing.T) {
	const n = 5000
	corpus, records := buildP3TextCorpus(t, n, 500, func(i int) string {
		return fmt.Sprintf("alpha beta gamma delta token%d", i%37)
	})
	defer releaseP3Records(records)

	ast, err := ParseAdvancedTextQuery("alpha OR beta OR token7")
	if err != nil {
		t.Fatal(err)
	}
	var cands []textScoreCandidate
	for shardIdx, shard := range corpus.shards {
		for i := range shard.docs {
			d := &shard.docs[i]
			ok, matched := ast.evaluate(d.lowerText, d.tokens, 0)
			if !ok {
				continue
			}
			cands = append(cands, textScoreCandidate{
				docIdx:  corpus.docBase[shardIdx] + i,
				tokens:  d.tokens,
				matched: matched,
			})
		}
	}
	if len(cands) < 64 {
		t.Fatalf("expected a batch-scored candidate set, got %d", len(cands))
	}

	batch := scoreTextCandidates(cands, corpus.nDocs, corpus.avgDL, corpus.df, DefaultBM25Tuning())
	if len(batch) != len(cands) {
		t.Fatalf("batch scores=%d want %d", len(batch), len(cands))
	}
	for i := range cands {
		scalar := scoreCachedDocBM25(cands[i].tokens, cands[i].matched, corpus.nDocs, corpus.avgDL, corpus.df, DefaultBM25Tuning())
		diff := math.Abs(batch[i] - scalar)
		tol := 1e-4*math.Abs(scalar) + 1e-6
		if diff > tol {
			t.Fatalf("candidate %d: SIMD score=%v scalar=%v (tol %v)", i, batch[i], scalar, tol)
		}
	}
}

// TestAdvancedTextShardReuse runs the test advanced text shard reuse operation.
func TestAdvancedTextShardReuse(t *testing.T) {
	records := []arrow.RecordBatch{
		buildP3TextBatch(0, 3, func(i int) string { return fmt.Sprintf("batch zero doc%d", i) }),
		buildP3TextBatch(3, 4, func(i int) string { return fmt.Sprintf("batch one doc%d", i) }),
		buildP3TextBatch(7, 2, func(i int) string { return fmt.Sprintf("batch two doc%d", i) }),
	}
	defer releaseP3Records(records)

	first, changed := buildTextCorpus(records, nil, nil)
	if !changed || first.nDocs != 9 {
		t.Fatalf("first build: changed=%v nDocs=%d", changed, first.nDocs)
	}
	if again, changedAgain := buildTextCorpus(records, nil, first); changedAgain || again != first {
		t.Fatalf("unchanged rebuild did not reuse the corpus (changed=%v)", changedAgain)
	}

	// Appending a batch must reuse the existing shards by pointer.
	appended := append([]arrow.RecordBatch(nil), records...)
	appended = append(appended, buildP3TextBatch(9, 5, func(i int) string { return fmt.Sprintf("batch three doc%d", i) }))
	defer appended[len(appended)-1].Release()
	second, changed := buildTextCorpus(appended, nil, first)
	if !changed || second.nDocs != 14 {
		t.Fatalf("append: changed=%v nDocs=%d", changed, second.nDocs)
	}
	for i := 0; i < len(first.shards); i++ {
		if second.shards[i] != first.shards[i] {
			t.Fatalf("append rebuilt unchanged shard %d", i)
		}
	}
	if second.shards[3] == nil || second.docBase[3] != 9 {
		t.Fatalf("appended shard base=%v, want 9", second.docBase)
	}

	// Deleting a row in the first batch rebuilds that shard and every later
	// shard (global vector IDs shift for none, but the shard contents changed),
	// while leaving the third batch's shard reusable only if its own rows are
	// unchanged and its vector base is unchanged.
	tombstones := map[int]*lbtypes.Bitset{0: lbtypes.NewBitset()}
	tombstones[0].Set(0)
	defer tombstones[0].Release()
	third, changed := buildTextCorpus(appended, tombstones, second)
	if !changed || third.nDocs != 13 {
		t.Fatalf("tombstone: changed=%v nDocs=%d", changed, third.nDocs)
	}
	if third.shards[0] == second.shards[0] {
		t.Fatal("tombstoned shard was reused")
	}
	for i := 1; i < len(second.shards); i++ {
		if third.shards[i] != second.shards[i] {
			t.Fatalf("shard %d after the tombstoned shard was rebuilt unnecessarily", i)
		}
	}
	if got, want := third.shards[1].docs[0].vectorID, uint32(3); got != want {
		t.Fatalf("vectorID after tombstone = %d, want %d", got, want)
	}

	got := searchP3Corpus(t, third, "batch", 20, 0)
	if got.total != 13 {
		t.Fatalf("tombstoned search total=%d want 13", got.total)
	}
	for _, id := range got.ids {
		if id == "0" {
			t.Fatal("deleted document 0 still returned")
		}
	}
}

// TestAdvancedTextNoCandidateCap runs the test advanced text no candidate cap operation.
func TestAdvancedTextNoCandidateCap(t *testing.T) {
	// A query whose only match sits beyond the old 50k evaluation cap must
	// still be found now that the cap is gone and exact candidates drive search.
	const n = 60_000
	corpus, records := buildP3TextCorpus(t, n, 10_000, func(i int) string {
		if i == n-1 {
			return "needle only here"
		}
		return "haystack filler text"
	})
	defer releaseP3Records(records)

	got := searchP3Corpus(t, corpus, "needle", 5, 0)
	if got.total != 1 || len(got.ids) != 1 || got.ids[0] != strconv.Itoa(n-1) {
		t.Fatalf("needle search = %v (total %d), want the final document", got.ids, got.total)
	}
}

// buildP3ScaleCorpus builds a catalog-shaped corpus: every document belongs to
// a category and carries one of 1000 SKUs. A SKU query is therefore selective
// (about 0.1% of the corpus) while still sharing a common category term.
func buildP3ScaleCorpus(tb testing.TB, n int) (*cachedTextCorpus, []arrow.RecordBatch) {
	return buildP3TextCorpus(tb, n, 50_000, func(i int) string {
		category := "electronics"
		if i%2 == 0 {
			category = "clothing"
		}
		// Pair every two documents onto the same SKU so SKUs are independent of
		// the category alternation; otherwise every sku42 document would be in
		// the same category and the AND below would have no hits.
		return category + " sku" + strconv.Itoa((i/2)%1000) + " red widget"
	})
}

func runP3Queries(corpus *cachedTextCorpus, ast queryNode, k, queries int) error {
	req := AdvancedTextQueryRequest{Dataset: "p3", Query: "sku42 AND electronics", K: k}
	for i := 0; i < queries; i++ {
		th := &textCandidateHeap{k: k}
		total := evaluateTextCorpus(corpus, ast, req, DefaultBM25Tuning(), th, map[string]map[string]int{})
		if total == 0 || th.Len() == 0 {
			return fmt.Errorf("query %d returned no hits (total=%d heap=%d)", i, total, th.Len())
		}
		heap.Init(th)
		for th.Len() > 0 {
			heap.Pop(th)
		}
	}
	return nil
}

func bestP3QPS(rounds, queries int, fn func() error) (float64, error) {
	best := 0.0
	for r := 0; r < rounds; r++ {
		start := time.Now()
		if err := fn(); err != nil {
			return 0, err
		}
		elapsed := time.Since(start).Seconds()
		if elapsed <= 0 {
			continue
		}
		if qps := float64(queries) / elapsed; qps > best {
			best = qps
		}
	}
	return best, nil
}

// TestAdvancedTextScalability guards P3-5b: a selective advanced-text query
// over a 1M-document corpus must sustain at least 500 QPS after warm-up.
func TestAdvancedTextScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping advanced-text scalability test in short mode")
	}
	const (
		n       = 1_000_000
		k       = 10
		queries = 100
		rounds  = 3
	)
	corpus, records := buildP3ScaleCorpus(t, n)
	defer releaseP3Records(records)
	if corpus.nDocs != n {
		t.Fatalf("corpus has %d docs, want %d", corpus.nDocs, n)
	}

	ast, err := ParseAdvancedTextQuery("sku42 AND electronics")
	if err != nil {
		t.Fatal(err)
	}
	// Warm the fuzzy/exact term cache and any lazy per-shard state.
	if err := runP3Queries(corpus, ast, k, 1); err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	qps, err := bestP3QPS(rounds, queries, func() error {
		return runP3Queries(corpus, ast, k, queries)
	})
	if err != nil {
		t.Fatalf("scale query: %v", err)
	}
	t.Logf("AdvancedTextScalability: n=%d query=%q qps=%.1f", n, "sku42 AND electronics", qps)
	if qps < 500 {
		t.Fatalf("advanced-text QPS %.1f below threshold 500", qps)
	}
}

// BenchmarkAdvancedTextScale measures the same hot path at the roadmap scales.
func BenchmarkAdvancedTextScale(b *testing.B) {
	for _, n := range []int{100_000, 500_000, 1_000_000} {
		if testing.Short() && n > 100_000 {
			b.Skip("skipping large advanced-text benchmark in short mode")
		}
		corpus, records := buildP3ScaleCorpus(b, n)
		ast, err := ParseAdvancedTextQuery("sku42 AND electronics")
		if err != nil {
			records[0].Release()
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("n=%d/selective", n), func(b *testing.B) {
			req := AdvancedTextQueryRequest{Dataset: "p3", Query: "sku42 AND electronics", K: 10}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				th := &textCandidateHeap{k: 10}
				if total := evaluateTextCorpus(corpus, ast, req, DefaultBM25Tuning(), th, map[string]map[string]int{}); total == 0 {
					b.Fatal("no hits")
				}
			}
			b.StopTimer()
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(b.N)/elapsed, "qps")
			}
		})
		releaseP3Records(records)
	}
}
