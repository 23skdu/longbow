package store

import (
	"container/heap"
)

// ResultHeap implements heap.Interface for a stream of SearchResults
type ResultHeap []StreamItem

type StreamItem struct {
	Result    SearchResult
	SourceIdx int // Index of the source channel
}

func (h ResultHeap) Len() int           { return len(h) }
func (h ResultHeap) Less(i, j int) bool { return h[i].Result.Score < h[j].Result.Score } // Min-heap by score (asc)
func (h ResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *ResultHeap) Push(x any) {
	*h = append(*h, x.(StreamItem))
}

func (h *ResultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeSortedStreams merges multiple sortedResult channels into a single sorted stream.
// It assumes input channels produce results sorted by Score (ascending).
// The output channel will also produce results sorted by Score.
func MergeSortedStreams(channels []<-chan []SearchResult, k int) <-chan SearchResult {
	out := make(chan SearchResult, k) // Buffer up to k

	go func() {
		defer close(out)

		h := &ResultHeap{}
		heap.Init(h)

		// 1. Initialize heap with the first available batch from each channel
		// Note: The input is []SearchResult (batches), not single items.
		// We need to buffer the current batch from each source.

		type SourceState struct {
			batch []SearchResult
			idx   int
			ch    <-chan []SearchResult
		}

		sources := make([]*SourceState, len(channels))

		for i, ch := range channels {
			sources[i] = &SourceState{ch: ch}
			// Fetch first batch
			if batch, ok := <-ch; ok && len(batch) > 0 {
				sources[i].batch = batch
				sources[i].idx = 0
				heap.Push(h, StreamItem{
					Result:    batch[0],
					SourceIdx: i,
				})
			}
		}

		count := 0
		for h.Len() > 0 && (k <= 0 || count < k) {
			// Pop smallest
			item := heap.Pop(h).(StreamItem)
			out <- item.Result
			count++

			// Push next from same source
			src := sources[item.SourceIdx]
			src.idx++

			if src.idx < len(src.batch) {
				// Next item in current batch
				heap.Push(h, StreamItem{
					Result:    src.batch[src.idx],
					SourceIdx: item.SourceIdx,
				})
			} else {
				// Fetch next batch from channel
				if batch, ok := <-src.ch; ok && len(batch) > 0 {
					src.batch = batch
					src.idx = 0
					heap.Push(h, StreamItem{
						Result:    batch[0],
						SourceIdx: item.SourceIdx,
					})
				}
				// Else channel closed or empty, source exhausted
			}
		}
	}()

	return out
}
