package store

import (
	"container/heap"
)

// mergeItem represents a search result from a specific shard during merging.
type mergeItem struct {
	result   SearchResult
	shardIdx int
	nextIdx  int // index of the next result in the shard's result slice
}

// mergeHeap is a min-heap of mergeItems, ordered by score (distance).
type mergeHeap []mergeItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].result.Score < h[j].result.Score }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(mergeItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// ShardedResultAggregator handles efficient merging of results from multiple shards.
type ShardedResultAggregator struct{}

// NewShardedResultAggregator creates a new result aggregator.
func NewShardedResultAggregator() *ShardedResultAggregator {
	return &ShardedResultAggregator{}
}

// MergeKWay performs a k-way merge on sorted results from multiple shards.
// It assumes each slice in shardResults is already sorted by Score ascending.
func (a *ShardedResultAggregator) MergeKWay(shardResults [][]SearchResult, k int) []SearchResult {
	if len(shardResults) == 0 {
		return nil
	}
	if len(shardResults) == 1 {
		res := shardResults[0]
		if len(res) > k {
			return res[:k]
		}
		return res
	}

	h := &mergeHeap{}
	heap.Init(h)

	// Initialize heap with the first result from each non-empty shard
	for i, res := range shardResults {
		if len(res) > 0 {
			heap.Push(h, mergeItem{
				result:   res[0],
				shardIdx: i,
				nextIdx:  1,
			})
		}
	}

	merged := make([]SearchResult, 0, k)
	for h.Len() > 0 && len(merged) < k {
		item := heap.Pop(h).(mergeItem)
		merged = append(merged, item.result)

		// If this shard has more results, push the next one onto the heap
		if item.nextIdx < len(shardResults[item.shardIdx]) {
			heap.Push(h, mergeItem{
				result:   shardResults[item.shardIdx][item.nextIdx],
				shardIdx: item.shardIdx,
				nextIdx:  item.nextIdx + 1,
			})
		}
	}

	return merged
}
