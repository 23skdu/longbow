package store

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// LockFreeSlice provides lock-free reads with copy-on-write updates for any slice type.
// This is an evolution of the LockFreeNeighborList designed for general use.
type LockFreeSlice[T any] struct {
	data          atomic.Pointer[[]T]
	activeReaders atomic.Int64
	currentEpoch  atomic.Uint64
	writeMu       sync.Mutex
}

// NewLockFreeSlice creates a new lock-free slice.
func NewLockFreeSlice[T any]() *LockFreeSlice[T] {
	l := &LockFreeSlice[T]{}
	empty := make([]T, 0)
	l.data.Store(&empty)
	return l
}

// Read returns the current slice without acquiring any locks.
func (l *LockFreeSlice[T]) Read() []T {
	l.activeReaders.Add(1)
	defer l.activeReaders.Add(-1)

	ptr := l.data.Load()
	if ptr == nil {
		return nil
	}
	return *ptr
}

// Update performs a copy-on-write update.
func (l *LockFreeSlice[T]) Update(newSlice []T) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	copied := make([]T, len(newSlice))
	copy(copied, newSlice)

	l.data.Store(&copied)
	l.waitForReaders()
	l.currentEpoch.Add(1)
}

// UpdateInPlace updates the slice without copying.
func (l *LockFreeSlice[T]) UpdateInPlace(newSlice []T) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	l.data.Store(&newSlice)
	l.waitForReaders()
	l.currentEpoch.Add(1)
}

func (l *LockFreeSlice[T]) waitForReaders() {
	for l.activeReaders.Load() > 0 {
		runtime.Gosched()
	}
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
	slice.Update(data)
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
