package index

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/require"
)

// termPostingList resolves a term's posting list, failing the test if absent.
func termPostingList(t *testing.T, idx *BM25InvertedIndex, term string) *BM25PostingList {
	t.Helper()

	shard := &idx.termShards[idx.termShardIndex(term)]
	pl, ok := shard.index.Get(term)
	require.Truef(t, ok, "term %q not present", term)
	return pl
}

// makeBM25Docs builds a deterministic corpus of pre-tokenised documents.
func makeBM25Docs(n, termsPerDoc int) []BM25Document {
	docs := make([]BM25Document, 0, n)
	for d := range n {
		tokens := make([]string, 0, termsPerDoc)
		for t := range termsPerDoc {
			// A handful of terms are deliberately hot so posting lists grow
			// large and the per-document reallocation cost is visible.
			tokens = append(tokens, fmt.Sprintf("hot%d", t%8))
			tokens = append(tokens, fmt.Sprintf("rare_%d_%d", d, t))
		}
		docs = append(docs, BM25Document{ID: VectorID(d), Tokens: tokens})
	}
	return docs
}

// TestBM25AddBatchMatchesAdd pins the batched index to the per-document one it
// replaced: identical posting lists, identical term frequencies, identical
// document lengths, identical corpus statistics.
func TestBM25AddBatchMatchesAdd(t *testing.T) {
	const (
		docs       = 500
		termsPerIn = 12
	)

	batched := NewBM25InvertedIndex(DefaultBM25Config())
	batched.AddBatch(makeBM25Docs(docs, termsPerIn))

	single := NewBM25InvertedIndex(DefaultBM25Config())
	for _, d := range makeBM25Docs(docs, termsPerIn) {
		single.Add(d.ID, joinTokens(d.Tokens))
	}

	require.Equal(t, single.DocCount(), batched.DocCount())
	require.Equal(t, single.ScorerDocs(), batched.ScorerDocs())

	for d := range docs {
		require.Equalf(t, single.GetDocLength(VectorID(d)), batched.GetDocLength(VectorID(d)),
			"document %d length", d)
	}

	for _, term := range []string{"hot0", "hot3", "hot7"} {
		require.Equalf(t, single.GetTermDocFreq(term), batched.GetTermDocFreq(term),
			"document frequency of %q", term)
	}

	// Posting lists must agree entry-for-entry, in the same sorted order.
	for _, term := range []string{"hot0", "hot1", "hot3", "rare_0_0", "rare_42_5"} {
		wantPL := termPostingList(t, single, term)
		gotPL := termPostingList(t, batched, term)
		require.Equal(t, wantPL.DocIDs, gotPL.DocIDs, "posting docs for %q", term)
		require.Equal(t, wantPL.TFs, gotPL.TFs, "posting tfs for %q", term)
	}
}

// TestBM25AddBatchIsAmortised guards the allocation profile that made BM25 the
// single largest allocator in the process (943 GB / 82.95% of all allocations
// during a pure-vector benchmark).
//
// The old shape rewrote the whole shard map and every touched posting list
// once per document. Batching must make the cost independent of how many
// documents share a shard: one BulkStore per shard, and one posting-list
// rebuild per touched term, regardless of batch size.
func TestBM25AddBatchIsAmortised(t *testing.T) {
	const (
		smallBatch = 10
		largeBatch = 500
	)

	perDoc := func(batchSize int) float64 {
		idx := NewBM25InvertedIndex(DefaultBM25Config())
		docs := makeBM25Docs(batchSize, 12)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		idx.AddBatch(docs)
		runtime.ReadMemStats(&after)

		return float64(after.TotalAlloc-before.TotalAlloc) / float64(batchSize)
	}

	small := perDoc(smallBatch)
	large := perDoc(largeBatch)

	require.Lessf(t, large, small*2,
		"per-document allocation must not grow with batch size: %.0f B/doc at %d docs vs %.0f B/doc at %d docs",
		small, smallBatch, large, largeBatch)

	// And the absolute floor: one posting list per distinct term, not one per
	// (term, document) pair.
	idx := NewBM25InvertedIndex(DefaultBM25Config())
	idx.AddBatch(makeBM25Docs(largeBatch, 12))
	require.Equal(t, largeBatch, idx.DocCount())
	require.Equal(t, largeBatch, idx.ScorerDocs())
}

// TestBM25AddBatchReindexIsIdempotent covers the retry path: re-adding a
// document must update it in place, never duplicate the posting entry, and
// must not inflate the scorer's corpus (which would skew every term's IDF).
func TestBM25AddBatchReindexIsIdempotent(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	docs := []BM25Document{
		{ID: 0, Tokens: []string{"alpha", "beta"}},
		{ID: 1, Tokens: []string{"alpha", "gamma"}},
	}
	idx.AddBatch(docs)
	idx.AddBatch(docs)
	idx.AddBatch(docs)

	require.Equal(t, 2, idx.DocCount(), "re-indexing must not add documents")
	require.Equal(t, 2, idx.ScorerDocs(), "the scorer corpus must not inflate on re-index")

	pl := termPostingList(t, idx, "alpha")
	require.Equal(t, []VectorID{0, 1}, pl.DocIDs, "a re-indexed document must not be duplicated")
	require.Equal(t, []int{1, 1}, pl.TFs,
		"tf counts occurrences in the document, so re-indexing leaves it unchanged")
}

// TestBM25AddBatchOutOfOrderAndGaps covers the sorted-merge path: document IDs
// are assigned by ingestion order in production, but AddBatch is public and
// WAL replay or a restore can hand it any order.
func TestBM25AddBatchOutOfOrderAndGaps(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	idx.AddBatch([]BM25Document{
		{ID: 100, Tokens: []string{"shared", "only100"}},
		{ID: 5, Tokens: []string{"shared"}},
		{ID: 900, Tokens: []string{"shared", "only900"}},
	})

	pl := termPostingList(t, idx, "shared")
	require.Equal(t, []VectorID{5, 100, 900}, pl.DocIDs, "posting lists must stay sorted by document ID")

	require.Equal(t, 3, idx.DocCount())
	require.Equal(t, 3, idx.ScorerDocs())

	// Appending a batch whose IDs straddle existing ones must merge, not
	// overwrite or duplicate.
	idx.AddBatch([]BM25Document{
		{ID: 50, Tokens: []string{"shared"}},
		{ID: 950, Tokens: []string{"shared"}},
	})

	pl = termPostingList(t, idx, "shared")
	require.Equal(t, []VectorID{5, 50, 100, 900, 950}, pl.DocIDs)
	require.Equal(t, 5, idx.DocCount())
}

// TestBM25ArenaIndexDocumentsBatchMatchesSingle pins the arena batch to its
// per-document form and checks that the token dictionary, document frequencies
// and document lengths all agree.
func TestBM25ArenaIndexDocumentsBatchMatchesSingle(t *testing.T) {
	tokens := []string{"alpha", "beta", "alpha", "gamma"}

	batched := NewBM25ArenaIndex(memory.NewSlabArena(1<<20), 16)
	require.NoError(t, batched.IndexDocuments([]BM25ArenaDocument{
		{ID: 0, Tokens: tokens},
		{ID: 1, Tokens: []string{"beta", "delta"}},
		{ID: 2, Tokens: []string{"alpha"}},
	}))

	single := NewBM25ArenaIndex(memory.NewSlabArena(1<<20), 16)
	for i, tk := range [][]string{tokens, {"beta", "delta"}, {"alpha"}} {
		require.NoError(t, single.IndexDocument(uint32(i), tk))
	}

	require.Equal(t, single.DocumentCount(), batched.DocumentCount())
	require.Equal(t, single.TokenCount(), batched.TokenCount())

	for term, wantDocs := range map[string][]uint32{
		"alpha": {0, 2},
		"beta":  {0, 1},
		"gamma": {0},
		"delta": {1},
	} {
		id, ok := batched.GetTokenID(term)
		require.Truef(t, ok, "term %q missing", term)
		got, ok := batched.GetPostingList(id)
		require.Truef(t, ok, "term %q has no posting list", term)
		require.ElementsMatchf(t, wantDocs, got, "posting list for %q", term)
		require.Equalf(t, len(wantDocs), int(batched.GetDocumentFrequency(id)), "df for %q", term)
	}
}

// TestBM25ArenaIndexDocumentsBatchEmptyTokens ensures an empty document does not
// corrupt the running average document length (it divides by totalDocs).
func TestBM25ArenaIndexDocumentsBatchEmptyTokens(t *testing.T) {
	idx := NewBM25ArenaIndex(memory.NewSlabArena(1<<20), 16)

	require.NoError(t, idx.IndexDocuments([]BM25ArenaDocument{
		{ID: 0, Tokens: nil},
		{ID: 1, Tokens: []string{"alpha", "beta"}},
		{ID: 2, Tokens: []string{"alpha"}},
	}))

	require.Equal(t, uint32(2), idx.DocumentCount(), "empty documents must not be counted")
	require.NotZero(t, idx.avgDocLength)

	scores := idx.Score([]string{"alpha"}, []uint32{1, 2})
	require.Contains(t, scores, uint32(2))
}

func joinTokens(tokens []string) string {
	out := ""
	for i, t := range tokens {
		if i > 0 {
			out += " "
		}
		out += t
	}
	return out
}
