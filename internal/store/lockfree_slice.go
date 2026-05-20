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

// NewLockFreeSliceFrom creates a new lock-free slice initialized with data.
func NewLockFreeSliceFrom[T any](data []T) *LockFreeSlice[T] {
	l := &LockFreeSlice[T]{}
	l.data.Store(&data)
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
 
// MapRCU provides a truly lock-free (read-side) map using Copy-On-Write.
// Optimized for very frequent reads and infrequent to moderate updates.
type MapRCU[K comparable, V any] struct {
	data    atomic.Value // stores map[K]V
	writeMu sync.Mutex
}
 
// NewMapRCU creates a new RCU-protected map.
func NewMapRCU[K comparable, V any]() *MapRCU[K, V] {
	m := &MapRCU[K, V]{}
	m.data.Store(make(map[K]V))
	return m
}
 
// Load returns the current map. The returned map MUST NOT be modified.
func (l *MapRCU[K, V]) Load() map[K]V {
	return l.data.Load().(map[K]V)
}
 
// Get retrieves a value from the map.
func (l *MapRCU[K, V]) Get(key K) (V, bool) {
	val, ok := l.Load()[key]
	return val, ok
}
 
// Store updates the map using a Copy-On-Write operation.
func (l *MapRCU[K, V]) Store(key K, val V) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
 
	oldMap := l.Load()
	newMap := make(map[K]V, len(oldMap)+1)
	for k, v := range oldMap {
		newMap[k] = v
	}
	newMap[key] = val
	l.data.Store(newMap)
}
 
// Delete removes a key from the map using COW.
func (l *MapRCU[K, V]) Delete(key K) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
 
	oldMap := l.Load()
	if _, ok := oldMap[key]; !ok {
		return
	}
 
	newMap := make(map[K]V, len(oldMap)-1)
	for k, v := range oldMap {
		if k != key {
			newMap[k] = v
		}
	}
	l.data.Store(newMap)
}
 
// Range iterates over the map. The map is a consistent snapshot.
func (l *MapRCU[K, V]) Range(f func(key K, val V) bool) {
	for k, v := range l.Load() {
		if !f(k, v) {
			break
		}
	}
}

// BulkStore updates multiple keys using a single Copy-On-Write operation.
func (l *MapRCU[K, V]) BulkStore(updates map[K]V) {
	if len(updates) == 0 {
		return
	}
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	oldMap := l.Load()
	newMap := make(map[K]V, len(oldMap)+len(updates))
	for k, v := range oldMap {
		newMap[k] = v
	}
	for k, v := range updates {
		newMap[k] = v
	}
	l.data.Store(newMap)
}

// BulkDelete removes multiple keys using COW.
func (l *MapRCU[K, V]) BulkDelete(keys []K) {
	if len(keys) == 0 {
		return
	}
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	oldMap := l.Load()
	newMap := make(map[K]V, len(oldMap))
	for k, v := range oldMap {
		newMap[k] = v
	}
	for _, k := range keys {
		delete(newMap, k)
	}
	l.data.Store(newMap)
}
