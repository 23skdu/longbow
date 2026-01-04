package store

import (
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// MeasuredMutex wraps sync.Mutex to record wait duration.
type MeasuredMutex struct {
	mu        sync.Mutex
	labelType string
}

// NewMeasuredMutex creates a new measured mutex.
// Ideally usage implies struct embedding or direct replacement,
// but since we can't easily init array of structs with args,
// we might need to rely on a lazy init or a factory.
// For shardedLocks, we might just set the label when using.
// But sync.Mutex zero value is valid. MeasuredMutex zero value has empty label.
// We should allow empty label or use a SetLabel method, or just hardcode "shard" if widely used there.
// Better: Store the label in the wrapper.
func NewMeasuredMutex(label string) MeasuredMutex {
	return MeasuredMutex{labelType: label}
}

// Lock records the duration spent waiting for the lock.
func (m *MeasuredMutex) Lock() {
	start := time.Now()
	m.mu.Lock()
	duration := time.Since(start)
	if duration > time.Microsecond { // Only record non-trivial waits to reduce overhead?
		// Or strictly record everything.
		// Prometheus observe is fast (atomic), but time.Now() has cost.
		if m.labelType != "" {
			metrics.LockContentionDuration.WithLabelValues(m.labelType).Observe(duration.Seconds())
		}
	}
}

// Unlock unlocks the mutex.
func (m *MeasuredMutex) Unlock() {
	m.mu.Unlock()
}

// MeasuredRWMutex wraps sync.RWMutex.
type MeasuredRWMutex struct {
	mu        sync.RWMutex
	labelType string
}

func NewMeasuredRWMutex(label string) MeasuredRWMutex {
	return MeasuredRWMutex{labelType: label}
}

func (m *MeasuredRWMutex) Lock() {
	start := time.Now()
	m.mu.Lock()
	if m.labelType != "" {
		metrics.LockContentionDuration.WithLabelValues(m.labelType + "_write").Observe(time.Since(start).Seconds())
	}
}

func (m *MeasuredRWMutex) Unlock() {
	m.mu.Unlock()
}

func (m *MeasuredRWMutex) RLock() {
	start := time.Now()
	m.mu.RLock()
	if m.labelType != "" {
		metrics.LockContentionDuration.WithLabelValues(m.labelType + "_read").Observe(time.Since(start).Seconds())
	}
}

func (m *MeasuredRWMutex) RUnlock() {
	m.mu.RUnlock()
}
