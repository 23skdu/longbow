package index

// Roadmap Item 31: two builds of the same corpus must produce the same graph
// and the same answers.
//
// The harness could only ever compare end-to-end digests, which say *that*
// two runs differ but not where. This test builds the same corpus twice, in
// the same process, through the same batched flush shape the store uses, and
// compares the graph itself - entry point, per-node level, adjacency of every
// layer - and then the answers to 500 fixed queries. It splits "the index is
// not deterministic" from "the pipeline around the index is not" and from "the
// search is not".
//
// The logged digest is stable across processes: same corpus, same seed, same
// binary -> same hex. Running the test twice and diffing the two digests is
// therefore the cross-process half of the same check.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// float32RecordBatch wraps a float32 corpus in the record the bulk path
// ingests: a single FixedSizeList column named "vector".
func float32RecordBatch(tb testing.TB, corpus [][]float32) arrow.RecordBatch {
	tb.Helper()
	dims := len(corpus[0])
	b := array.NewRecordBuilder(memory.NewGoAllocator(), arrow.NewSchema(
		[]arrow.Field{{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)}}, nil))
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	vb := listB.ValueBuilder().(*array.Float32Builder)
	for _, v := range corpus {
		listB.Append(true)
		vb.AppendValues(v, nil)
	}
	rec := b.NewRecordBatch()
	rec.Retain()
	tb.Cleanup(rec.Release)
	return rec
}

// graphDigest hashes everything about an index that a rebuild is supposed to
// reproduce: metadata, every node's neighbours on every layer, and the
// location table the search path uses to turn an internal id back into a row.
// Order is fixed (node id ascending, layer ascending) so the hex is comparable
// across runs and across processes.
func graphDigest(tb testing.TB, idx *ArrowHNSW) string {
	tb.Helper()
	h := sha256.New()
	var num [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(num[:], v)
		_, _ = h.Write(num[:])
	}

	meta := idx.GetMetadataSnapshot()
	put(uint64(meta.EntryPoint)) // #nosec G115 -- digesting, not counting
	put(uint64(meta.MaxLevel))   // #nosec G115
	put(uint64(meta.NodeCount))  // #nosec G115

	maxLevel := int(meta.MaxLevel)
	if maxLevel < 0 {
		maxLevel = 0
	}
	for id := uint32(0); id < uint32(meta.NodeCount); id++ {
		for layer := 0; layer <= maxLevel; layer++ {
			nb, err := idx.GetLayerNeighbors(id, layer)
			if err != nil {
				// An error is part of the observable state: which nodes have
				// which layer must not move between builds either.
				put(0xFFFFFFFFFFFFFFFF)
				continue
			}
			put(uint64(len(nb))) // #nosec G115
			for _, v := range nb {
				put(uint64(v)) // #nosec G115
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// answerDigest hashes the answers to a fixed query set: hit ids and score
// bits, in the order the index returned them. Float scores go in as their
// IEEE-754 bit pattern, so a difference in the last mantissa bit counts.
func answerDigest(tb testing.TB, idx *ArrowHNSW, queries [][]float32, k int) (string, int) {
	tb.Helper()
	h := sha256.New()
	var num [8]byte
	putU := func(v uint64) {
		binary.LittleEndian.PutUint64(num[:], v)
		_, _ = h.Write(num[:])
	}
	putF := func(v float32) { putU(uint64(math.Float32bits(v))) } // #nosec G115

	hits := 0
	for qi, q := range queries {
		putU(uint64(qi)) // #nosec G115
		res, err := idx.SearchVectors(context.Background(), q, k, nil, types.SearchOptions{})
		if err != nil {
			putU(0xDEAD)
			continue
		}
		hits += len(res)
		putU(uint64(len(res))) // #nosec G115
		for _, r := range res {
			putU(uint64(r.ID)) // #nosec G115
			putF(r.Score)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), hits
}

// TestBulkBuildIsRepeatable builds the same 4k corpus twice and requires the
// two indexes to be indistinguishable: same graph, same answers.
func TestBulkBuildIsRepeatable(t *testing.T) {
	const dims, n, batch = 128, 4000, 1000

	corp := uniformCorpus(t, 20261016, n, dims)
	rec := float32RecordBatch(t, corp)
	cfg := chainLinkTestConfig(types.VectorTypeFloat32, dims)

	first := buildThroughBulk(t, rec, n, batch, cfg)
	second := buildThroughBulk(t, rec, n, batch, cfg)

	g1, g2 := graphDigest(t, first), graphDigest(t, second)
	if g1 != g2 {
		// Narrow the blast radius: which layer, which kind of difference.
		m1, m2 := first.GetMetadataSnapshot(), second.GetMetadataSnapshot()
		if m1.EntryPoint != m2.EntryPoint || m1.MaxLevel != m2.MaxLevel || m1.NodeCount != m2.NodeCount {
			t.Errorf("metadata differs between two builds of one corpus: first entry=%d level=%d nodes=%d, second entry=%d level=%d nodes=%d",
				m1.EntryPoint, m1.MaxLevel, m1.NodeCount, m2.EntryPoint, m2.MaxLevel, m2.NodeCount)
		}
		shown := 0
		for id := uint32(0); id < n && shown < 5; id++ {
			for layer := 0; layer <= 3; layer++ {
				a, aerr := first.GetLayerNeighbors(id, layer)
				b, berr := second.GetLayerNeighbors(id, layer)
				if (aerr == nil) != (berr == nil) {
					t.Errorf("node %d layer %d: first returns err=%v, second err=%v", id, layer, aerr, berr)
					shown++
					break
				}
				if aerr != nil {
					continue
				}
				if len(a) != len(b) {
					t.Errorf("node %d layer %d: degree %d vs %d across two builds", id, layer, len(a), len(b))
					shown++
					break
				}
				for i := range a {
					if a[i] != b[i] {
						t.Errorf("node %d layer %d: neighbour[%d] %d vs %d across two builds", id, layer, i, a[i], b[i])
						shown++
						break
					}
				}
				if shown >= 5 {
					break
				}
			}
		}
	}
	t.Logf("BULK_GRAPH_DIGEST %s", g1)

	// 500 fixed queries: the shape the harness verification uses.
	qRng := rand.New(rand.NewSource(4242)) // #nosec G404 -- fixed query set
	queries := make([][]float32, 500)
	for i := range queries {
		q := make([]float32, dims)
		for j := range q {
			q[j] = qRng.Float32()*2 - 1
		}
		queries[i] = q
	}

	a1, hits1 := answerDigest(t, first, queries, 10)
	a2, hits2 := answerDigest(t, second, queries, 10)
	if a1 != a2 {
		t.Errorf("two builds of one corpus answer differently: %s vs %s (hits %d vs %d)",
			a1, a2, hits1, hits2)
	}
	t.Logf("BULK_ANSWER_DIGEST %s", a1)

	// Both halves have to say something: two empty indexes agree trivially,
	// and a search that errors on every query agrees with itself too.
	if hits1 == 0 {
		t.Error("no query returned a hit: the digest compares two nothing-answers")
	}
	if a1 == sha256Empty || g1 == sha256Empty {
		t.Error("digest is of zero bytes: the build produced an empty index")
	}
}

// sha256Empty is sha256 over zero bytes, the value a digest takes when there
// was nothing to hash.
const sha256Empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
