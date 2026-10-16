package types

import (
	"sync"
	"sync/atomic"
)

// LockFreeSlice provides lock-free reads with copy-on-write updates for any slice type.
// This is an evolution of the LockFreeNeighborList designed for general use.
type LockFreeSlice[T any] struct {
	data         atomic.Pointer[[]T]
	currentEpoch atomic.Uint64
	writeMu      sync.Mutex
}

// NewLockFreeSlice creates a new lock-free slice.
func NewLockFreeSlice[T any]() *LockFreeSlice[T] {
	l := &LockFreeSlice[T]{}
	empty := make([]T, 0)
	l.data.Store(&empty)
	return l
}

// NewLockFreeSliceFrom creates a new lock-free slice initialized with data.
func NewLockFreeSliceFrom[T any](data []T) *LockFreeSlice[T] {
	l := &LockFreeSlice[T]{}
	l.data.Store(&data)
	return l
}

// Read returns the current slice without acquiring any locks.
func (l *LockFreeSlice[T]) Read() []T {
	ptr := l.data.Load()
	if ptr == nil {
		return nil
	}
	return *ptr
}

// Append adds an element to the end of the slice and returns the new length.
//
// Growth is geometric, so appending N elements costs O(N) in total instead of
// the O(N^2) an external `make`/`copy` on every append costs. When the backing
// array already has spare capacity the element is written past the *published*
// length: a reader that already loaded the old slice header can only observe
// [0:len], so an in-flight append is invisible to it and the array needs no
// copy at all.
func (l *LockFreeSlice[T]) Append(item T) int {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	var old []T
	if cur := l.data.Load(); cur != nil {
		old = *cur
	}

	n := len(old) + 1
	if n <= cap(old) {
		grown := old[:n]
		grown[n-1] = item
		l.data.Store(&grown)
		l.currentEpoch.Add(1)
		return n
	}

	next := make([]T, n, appendGrowth(n))
	copy(next, old)
	next[n-1] = item
	l.data.Store(&next)
	l.currentEpoch.Add(1)
	return n
}

// appendGrowth returns the capacity to reserve for an append that reaches
// length n. Batches accumulate one entry at a time for the lifetime of a
// dataset, so the reserve is deliberately generous: the amortised copy cost of
// doubling is negligible next to the number of ingest batches it removes.
func appendGrowth(n int) int {
	const maxInt = int(^uint(0) >> 1)

	c := 32
	for c < n && c < maxInt/2 {
		c *= 2
	}
	return c
}

// Len returns the current number of elements. It is a single atomic load, so
// unlike summing a caller-maintained counter it cannot drift.
func (l *LockFreeSlice[T]) Len() int {
	ptr := l.data.Load()
	if ptr == nil {
		return 0
	}
	return len(*ptr)
}

// Update performs a copy-on-write update.
func (l *LockFreeSlice[T]) Update(newSlice []T) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	copied := make([]T, len(newSlice))
	copy(copied, newSlice)

	l.data.Store(&copied)
	l.currentEpoch.Add(1)
}

// UpdateInPlace updates the slice without copying.
func (l *LockFreeSlice[T]) UpdateInPlace(newSlice []T) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	l.data.Store(&newSlice)
	l.currentEpoch.Add(1)
}

// LockFreeMap is a thread-safe map of lock-free slices.
type LockFreeMap[K comparable, T any] struct {
	slices map[K]*LockFreeSlice[T]
	mu     sync.RWMutex
}

// NewLockFreeMap creates a new thread-safe map for managing multiple lock-free slices.
func NewLockFreeMap[K comparable, T any]() *LockFreeMap[K, T] {
	return &LockFreeMap[K, T]{
		slices: make(map[K]*LockFreeSlice[T]),
	}
}

func (l *LockFreeMap[K, T]) Get(key K) ([]T, bool) {
	l.mu.RLock()
	slice, ok := l.slices[key]
	l.mu.RUnlock()

	if !ok {
		return nil, false
	}
	return slice.Read(), true
}

func (l *LockFreeMap[K, T]) Set(key K, data []T) {
	l.mu.RLock()
	slice, ok := l.slices[key]
	l.mu.RUnlock()

	if !ok {
		l.mu.Lock()
		slice, ok = l.slices[key]
		if !ok {
			slice = NewLockFreeSlice[T]()
			l.slices[key] = slice
		}
		l.mu.Unlock()
	}
	slice.UpdateInPlace(data)
}

func (l *LockFreeMap[K, T]) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.slices)
}

func (l *LockFreeMap[K, T]) Keys() []K {
	l.mu.RLock()
	defer l.mu.RUnlock()
	keys := make([]K, 0, len(l.slices))
	for k := range l.slices {
		keys = append(keys, k)
	}
	return keys
}
