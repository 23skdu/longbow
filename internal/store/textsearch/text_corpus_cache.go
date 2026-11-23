package textsearch

import (
	"encoding/json"
	"fmt"
	"github.com/23skdu/longbow/internal/store/textutil"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
)

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

func (m *AdvancedTextSearchManager) corpusFor(dataset string) (*cachedTextCorpus, error) {
	records, tombstones, err := m.store.TextCorpus(dataset)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	prev := m.corpusCache[dataset]
	m.mu.RUnlock()

	corpus, changed := buildTextCorpus(records, tombstones, prev)

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
		idVal := textutil.ReadRowStringValue(rec, colIdx, meta, r, "id")
		if idVal == "" {
			idVal = strconv.FormatUint(uint64(vectorID), 10)
		}
		text := collectDocumentText(rec, colIdx, meta, r)
		lowerText := strings.ToLower(text)
		tokens := textutil.TokenizeAlphaNum(text)
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
