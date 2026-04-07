package autoscale

import (
	"sync"
	"time"
)

// RollingWindow tracks counts over a sliding time window.
type RollingWindow struct {
	mu         sync.RWMutex
	buckets    []int64
	lastUpdate time.Time
	interval   time.Duration
	capacity   int
}

// NewRollingWindow creates a new rolling window with total capacity of interval * capacity
func NewRollingWindow(interval time.Duration, capacity int) *RollingWindow {
	return &RollingWindow{
		buckets:    make([]int64, capacity),
		interval:   interval,
		capacity:   capacity,
		lastUpdate: time.Now(),
	}
}

// Add increments the current bucket by the given delta.
func (rw *RollingWindow) Add(delta int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.advance()
	rw.buckets[0] += delta
}

// Sum returns the total sum of all buckets in the window.
func (rw *RollingWindow) Sum() int64 {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	// We don't advance for a read, but we should conceptually
	// know where we are.
	var sum int64
	now := time.Now()
	diff := int(now.Sub(rw.lastUpdate) / rw.interval)

	if diff >= rw.capacity {
		return 0
	}

	for i := diff; i < rw.capacity; i++ {
		sum += rw.buckets[i-diff]
	}

	return sum
}

// Average returns the average value per interval over the window.
func (rw *RollingWindow) Average() float64 {
	return float64(rw.Sum()) / float64(rw.capacity)
}

// advance slides the window based on current time.
func (rw *RollingWindow) advance() {
	now := time.Now()
	diff := int(now.Sub(rw.lastUpdate) / rw.interval)
	if diff == 0 {
		return
	}

	if diff >= rw.capacity {
		// Reset everything
		for i := range rw.buckets {
			rw.buckets[i] = 0
		}
	} else {
		// Shift buckets
		copy(rw.buckets[diff:], rw.buckets[:rw.capacity-diff])
		for i := 0; i < diff; i++ {
			rw.buckets[i] = 0
		}
	}

	rw.lastUpdate = now.Truncate(rw.interval)
}
