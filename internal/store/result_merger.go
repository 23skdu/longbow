package store

import (
	"container/heap"
	"sync"
)

var searchResultPool = sync.Pool{
	New: func() any {
		return make([]SearchResult, 0, 1000)
	},
}

// GetSearchResultSlice retrieves a pre-allocated slice from the pool
func GetSearchResultSlice(capacity int) []SearchResult {
	slice := searchResultPool.Get().([]SearchResult)
	if cap(slice) < capacity {
		return make([]SearchResult, 0, capacity)
	}
	return slice[:0]
}

// PutSearchResultSlice returns a slice to the pool
func PutSearchResultSlice(slice []SearchResult) {
	searchResultPool.Put(slice[:0])
}

// ResultHeap implements heap.Interface for a stream of SearchResults
type ResultHeap []StreamItem

// StreamItem represents a single search result within a stream, tracking its source.
type StreamItem struct {
	Result    SearchResult
	SourceIdx int // Index of the source channel
}

func (h ResultHeap) Len() int           { return len(h) }
func (h ResultHeap) Less(i, j int) bool { return h[i].Result.Score < h[j].Result.Score } // Min-heap by score (asc)
func (h ResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push adds a StreamItem to the heap.
func (h *ResultHeap) Push(x any) {
	*h = append(*h, x.(StreamItem))
}

// Pop removes and returns the smallest StreamItem from the heap.
func (h *ResultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeSortedStreams merges multiple sortedResult channels into a single sorted stream.
// It assumes input channels produce results sorted by Score (ascending).
func MergeSortedStreams(channels []<-chan []SearchResult, k int) []SearchResult {
	if k <= 0 {
		return nil
	}
	
	out := GetSearchResultSlice(k)

	h := &ResultHeap{}
	heap.Init(h)

	type SourceState struct {
		batch []SearchResult
		idx   int
		ch    <-chan []SearchResult
	}

	sources := make([]*SourceState, len(channels))

	for i, ch := range channels {
		sources[i] = &SourceState{ch: ch}
		if batch, ok := <-ch; ok && len(batch) > 0 {
			sources[i].batch = batch
			sources[i].idx = 0
			heap.Push(h, StreamItem{
				Result:    batch[0],
				SourceIdx: i,
			})
		}
	}

	for h.Len() > 0 && len(out) < k {
		item := heap.Pop(h).(StreamItem)
		out = append(out, item.Result)

		src := sources[item.SourceIdx]
		src.idx++

		if src.idx < len(src.batch) {
			heap.Push(h, StreamItem{
				Result:    src.batch[src.idx],
				SourceIdx: item.SourceIdx,
			})
		} else {
			if batch, ok := <-src.ch; ok && len(batch) > 0 {
				src.batch = batch
				src.idx = 0
				heap.Push(h, StreamItem{
					Result:    batch[0],
					SourceIdx: item.SourceIdx,
				})
			}
		}
	}

	return out
}
