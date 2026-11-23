package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The merge contract: inputs are best-first (Score descending, highest first)
// — the order produced by vector search, graph re-ranking, and peer DoGet
// streams — and the output is the best-first top-k across all streams.

// TestMergeSortedStreams runs the test merge sorted streams operation.
func TestMergeSortedStreams(t *testing.T) {
	// Scenario 1: Basic merge of 3 best-first streams
	ch1 := make(chan []SearchResult, 3)
	ch2 := make(chan []SearchResult, 3)
	ch3 := make(chan []SearchResult, 3)

	go func() {
		ch1 <- []SearchResult{{ID: 1, Score: 0.7}, {ID: 2, Score: 0.4}}
		ch1 <- []SearchResult{{ID: 3, Score: 0.1}}
		close(ch1)
	}()

	go func() {
		ch2 <- []SearchResult{{ID: 4, Score: 0.8}, {ID: 5, Score: 0.5}}
		close(ch2)
	}()

	go func() {
		ch3 <- []SearchResult{{ID: 8, Score: 0.9}, {ID: 6, Score: 0.6}}
		ch3 <- []SearchResult{{ID: 7, Score: 0.3}}
		close(ch3)
	}()

	results := MergeSortedStreams([]<-chan []SearchResult{ch1, ch2, ch3}, 10)

	require.Equal(t, 8, len(results))
	// Global best-first ordering: 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.1
	expected := []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.1}
	for i, want := range expected {
		assert.InDelta(t, want, results[i].Score, 1e-6, "position %d", i)
	}
}

// TestMergeSortedStreams_EmptyChannels runs the test merge sorted streams_empty channels operation.
func TestMergeSortedStreams_EmptyChannels(t *testing.T) {
	ch1 := make(chan []SearchResult)
	close(ch1)

	ch2 := make(chan []SearchResult, 1)
	ch2 <- []SearchResult{{ID: 1, Score: 0.5}}
	close(ch2)

	results := MergeSortedStreams([]<-chan []SearchResult{ch1, ch2}, 5)

	assert.Equal(t, 1, len(results))
	assert.Equal(t, float32(0.5), results[0].Score)
}

// TestMergeSortedStreams_LimitK runs the test merge sorted streams_limit k operation.
func TestMergeSortedStreams_LimitK(t *testing.T) {
	ch1 := make(chan []SearchResult, 1)

	go func() {
		ch1 <- []SearchResult{{ID: 1, Score: 0.9}, {ID: 2, Score: 0.8}, {ID: 3, Score: 0.7}}
		close(ch1)
	}()

	// Request only the top 2 (the two best scores, not the last two)
	results := MergeSortedStreams([]<-chan []SearchResult{ch1}, 2)
	require.Equal(t, 2, len(results))
	assert.Equal(t, uint32(1), uint32(results[0].ID))
	assert.Equal(t, uint32(2), uint32(results[1].ID))
}

// TestMergeSortedStreams_InterleavedDelays runs the test merge sorted streams_interleaved delays operation.
func TestMergeSortedStreams_InterleavedDelays(t *testing.T) {
	ch1 := make(chan []SearchResult)
	ch2 := make(chan []SearchResult)

	go func() {
		ch1 <- []SearchResult{{ID: 1, Score: 0.9}}
		time.Sleep(50 * time.Millisecond)
		ch1 <- []SearchResult{{ID: 2, Score: 0.1}}
		close(ch1)
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		ch2 <- []SearchResult{{ID: 3, Score: 0.8}}
		ch2 <- []SearchResult{{ID: 4, Score: 0.5}}
		close(ch2)
	}()

	results := MergeSortedStreams([]<-chan []SearchResult{ch1, ch2}, 10)

	// 0.9, 0.8, 0.5, 0.1
	expectedScores := []float32{0.9, 0.8, 0.5, 0.1}
	require.Equal(t, len(expectedScores), len(results))
	for i, s := range expectedScores {
		assert.Equal(t, s, results[i].Score)
	}
}

// TestMergeSortedStreams_BestFirstAcrossStreams is the GlobalSearch accuracy
// regression: a local best-first stream merged with a peer best-first stream
// must interleave by score and keep the global best, not the local best.
func TestMergeSortedStreams_BestFirstAcrossStreams(t *testing.T) {
	local := make(chan []SearchResult, 1)
	peer := make(chan []SearchResult, 1)

	local <- []SearchResult{{ID: 100, Score: 0.99}, {ID: 101, Score: 0.7}, {ID: 102, Score: 0.5}}
	close(local)
	peer <- []SearchResult{{ID: 10, Score: 1.0}, {ID: 20, Score: 0.6}}
	close(peer)

	results := MergeSortedStreams([]<-chan []SearchResult{local, peer}, 3)

	require.Equal(t, 3, len(results))
	// The peer's global best (1.0) must come before the local best (0.99).
	assert.Equal(t, uint32(10), uint32(results[0].ID))
	assert.Equal(t, uint32(100), uint32(results[1].ID))
	// Third is the next best across streams: local 0.7 beats peer 0.6.
	assert.Equal(t, uint32(101), uint32(results[2].ID))
	for i := 1; i < len(results); i++ {
		assert.GreaterOrEqual(t, results[i-1].Score, results[i].Score, "output must be best-first")
	}
}

// TestMergeSortedStreams_KZeroReturnsNil runs the test merge sorted streams_k zero returns nil operation.
func TestMergeSortedStreams_KZeroReturnsNil(t *testing.T) {
	ch := make(chan []SearchResult, 1)
	ch <- []SearchResult{{ID: 1, Score: 0.5}}
	close(ch)
	require.Nil(t, MergeSortedStreams([]<-chan []SearchResult{ch}, 0))
}

// FuzzMergeSortedStreams verifies the k-way merge against a brute-force
// reference: the output must be exactly the global top-k (best-first) of the
// union of all input streams, for arbitrary stream counts, sizes, and k.
func FuzzMergeSortedStreams(f *testing.F) {
	f.Add(int64(42), 3, 5, 3)
	f.Add(int64(7), 1, 10, 10)
	f.Add(int64(99), 8, 0, 5)
	f.Add(int64(0), 2, 4, 100)

	f.Fuzz(func(t *testing.T, seed int64, numStreams, itemsPerStream, k int) {
		if numStreams < 1 || numStreams > 8 {
			t.Skip()
		}
		if itemsPerStream < 0 || itemsPerStream > 64 {
			t.Skip()
		}
		if k < 0 || k > 512 {
			t.Skip()
		}

		// Build a pool of unique scores, sorted best-first (descending).
		total := numStreams * itemsPerStream
		all := make([]SearchResult, 0, total)
		for i := 0; i < total; i++ {
			// Unique, strictly decreasing scores.
			all = append(all, SearchResult{ID: VectorID(i), Score: float32(total-i) / float32(total+1)})
		}

		// Partition the global best-first list into numStreams contiguous
		// chunks so every stream is itself sorted best-first and the union of
		// the streams is exactly `all`.
		channels := make([]<-chan []SearchResult, 0, numStreams)
		pos := 0
		for s := 0; s < numStreams; s++ {
			chunk := make([]SearchResult, 0, itemsPerStream)
			for i := 0; i < itemsPerStream && pos < total; i++ {
				chunk = append(chunk, all[pos])
				pos++
			}
			ch := make(chan []SearchResult, 1)
			if len(chunk) > 0 {
				ch <- chunk
			}
			close(ch)
			channels = append(channels, ch)
		}
		_ = seed // ordering is fully deterministic; seed reserved for future jitter

		results := MergeSortedStreams(channels, k)

		// Expected: the global top-k, best-first.
		wantLen := k
		if wantLen > total {
			wantLen = total
		}
		if k == 0 {
			wantLen = 0
		}
		if len(results) != wantLen {
			t.Fatalf("expected %d results, got %d", wantLen, len(results))
		}
		for i := 0; i < len(results); i++ {
			if results[i].ID != all[i].ID {
				t.Fatalf("position %d: expected ID %d (score %v), got ID %d (score %v)",
					i, all[i].ID, all[i].Score, results[i].ID, results[i].Score)
			}
			if i > 0 && results[i-1].Score < results[i].Score {
				t.Fatalf("output not best-first at position %d: %v < %v", i, results[i-1].Score, results[i].Score)
			}
		}
	})
}
