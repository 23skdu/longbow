package core

import (
	"sync/atomic"
	"math/rand/v2"
)

const (
	maxSkipListLevel = 16
	skipListP        = 0.5
)

// skipListNode represents a node in the concurrent skip list.
type skipListNode struct {
	key   uint32
	level int
	next  []atomic.Pointer[skipListNode]
}

// ConcurrentSkipList provides a lock-free (mostly CAS-based) skip list
// intended for maintaining HNSW layer entry points and high-throughput lookups.
type ConcurrentSkipList struct {
	head *skipListNode
}

// NewConcurrentSkipList creates a new lock-free skip list.
func NewConcurrentSkipList() *ConcurrentSkipList {
	return &ConcurrentSkipList{
		head: &skipListNode{
			next: make([]atomic.Pointer[skipListNode], maxSkipListLevel),
		},
	}
}

// Insert adds a key to the skip list using optimistic locking/CAS.
func (sl *ConcurrentSkipList) Insert(key uint32) {
	level := sl.randomLevel()
	newNode := &skipListNode{
		key:   key,
		level: level,
		next:  make([]atomic.Pointer[skipListNode], level+1),
	}

	for {
		preds, succs := sl.find(key)
		// Check if already exists (optional, depends on usecase)
		if succs[0] != nil && succs[0].key == key {
			return
		}

		for i := 0; i <= level; i++ {
			newNode.next[i].Store(succs[i])
		}

		// CAS the bottom level first
		pred := preds[0]
		succ := succs[0]
		if !pred.next[0].CompareAndSwap(succ, newNode) {
			continue // Lost race at bottom level, retry
		}

		// Propagate to upper levels
		for i := 1; i <= level; i++ {
			for {
				pred := preds[i]
				succ := succs[i]
				if pred.next[i].CompareAndSwap(succ, newNode) {
					break
				}
				// Re-find if CAS fails
				preds, succs = sl.find(key)
			}
		}
		return
	}
}

// Contains checks if the key exists in the skip list (lock-free).
func (sl *ConcurrentSkipList) Contains(key uint32) bool {
	curr := sl.head
	for i := maxSkipListLevel - 1; i >= 0; i-- {
		for {
			next := curr.next[i].Load()
			if next == nil || next.key >= key {
				if next != nil && next.key == key {
					return true
				}
				break
			}
			curr = next
		}
	}
	return false
}

// find returns the predecessors and successors for a key at each level.
func (sl *ConcurrentSkipList) find(key uint32) (preds []*skipListNode, succs []*skipListNode) {
	preds = make([]*skipListNode, maxSkipListLevel)
	succs = make([]*skipListNode, maxSkipListLevel)
	curr := sl.head
	for i := maxSkipListLevel - 1; i >= 0; i-- {
		for {
			next := curr.next[i].Load()
			if next == nil || next.key >= key {
				preds[i] = curr
				succs[i] = next
				break
			}
			curr = next
		}
	}
	return preds, succs
}

func (sl *ConcurrentSkipList) randomLevel() int {
	lvl := 0
	for rand.Float64() < skipListP && lvl < maxSkipListLevel-1 { // #nosec G404
		lvl++
	}
	return lvl
}

// GetRandom returns a random entry point from the list (lock-free).
func (sl *ConcurrentSkipList) GetRandom() (uint32, bool) {
	// Simple approach: follow Level 0 some random number of steps
	curr := sl.head.next[0].Load()
	if curr == nil {
		return 0, false
	}
	steps := rand.IntN(32) // #nosec G404
	for i := 0; i < steps; i++ {
		next := curr.next[0].Load()
		if next == nil {
			break
		}
		curr = next
	}
	return curr.key, true
}
