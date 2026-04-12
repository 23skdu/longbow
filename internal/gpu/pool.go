package gpu

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
)

// GPUIndexPool manages a pool of reusable GPU indexes
// This reduces allocation overhead and enables resource limiting
type GPUIndexPool struct {
	mu sync.RWMutex

	// Pool configuration
	maxSize       int           // Maximum number of indexes in pool
	maxConcurrent int           // Maximum concurrent active indexes
	idleTimeout   time.Duration // How long an idle index can remain in pool

	// Pool state
	idle         *list.List // Idle indexes available for reuse
	active       int        // Currently active (checked out) indexes
	waiting      int        // Requests waiting for an index
	totalCreated int        // Total indexes created
	totalReused  int        // Total times indexes were reused

	// Index factory
	createFunc func(types.GPUConfig) (types.Index, error)

	// Shutdown flag
	closed bool
}

// GPUIndexPoolConfig configures the GPU index pool
type GPUIndexPoolConfig struct {
	MaxSize       int
	MaxConcurrent int
	IdleTimeout   time.Duration
}

// DefaultGPUIndexPoolConfig returns default pool configuration
func DefaultGPUIndexPoolConfig() GPUIndexPoolConfig {
	return GPUIndexPoolConfig{
		MaxSize:       10,
		MaxConcurrent: 5,
		IdleTimeout:   5 * time.Minute,
	}
}

// pooledIndex wraps an index with pool metadata
type pooledIndex struct {
	index      types.Index
	config     types.GPUConfig
	createdAt  time.Time
	lastUsedAt time.Time
	useCount   int
}

// NewGPUIndexPool creates a new GPU index pool
func NewGPUIndexPool(config GPUIndexPoolConfig) *GPUIndexPool {
	return &GPUIndexPool{
		maxSize:       config.MaxSize,
		maxConcurrent: config.MaxConcurrent,
		idleTimeout:   config.IdleTimeout,
		idle:          list.New(),
		createFunc:    NewIndexWithConfig,
	}
}

// GetGPUIndex gets a GPU index from the pool or creates a new one
func (p *GPUIndexPool) GetGPUIndex(config types.GPUConfig) (types.Index, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("pool is closed")
	}

	// Check if we have an idle index with matching config
	for e := p.idle.Front(); e != nil; e = e.Next() {
		pi := e.Value.(*pooledIndex)
		if p.matchesConfig(pi.config, config) && !p.isExpired(pi) {
			// Move from idle to active
			p.idle.Remove(e)
			p.active++
			pi.lastUsedAt = time.Now()
			pi.useCount++
			p.totalReused++
			return &pooledIndexWrapper{pool: p, pooled: pi}, nil
		}
	}

	// Check if we can create a new index
	if p.active >= p.maxConcurrent {
		// Pool is at capacity
		return nil, &types.GPUNotAvailableError{
			Reason: fmt.Sprintf("max concurrent GPU indexes reached (%d)", p.maxConcurrent),
		}
	}

	// Create new index
	idx, err := p.createFunc(config)
	if err != nil {
		return nil, err
	}

	p.active++
	p.totalCreated++

	pi := &pooledIndex{
		index:      idx,
		config:     config,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		useCount:   1,
	}

	return &pooledIndexWrapper{pool: p, pooled: pi}, nil
}

// ReturnGPUIndex returns a GPU index to the pool
func (p *GPUIndexPool) ReturnGPUIndex(index types.Index) error {
	wrapper, ok := index.(*pooledIndexWrapper)
	if !ok {
		// Not from our pool, just close it
		return index.Close()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		// Pool is closed, close the index
		p.active--
		return wrapper.pooled.index.Close()
	}

	p.active--

	// Check if we should keep this index or close it
	if p.idle.Len() >= p.maxSize || p.isExpired(wrapper.pooled) {
		// Pool is full or index expired, close it
		return wrapper.pooled.index.Close()
	}

	// Add to idle pool
	wrapper.pooled.lastUsedAt = time.Now()
	p.idle.PushBack(wrapper.pooled)

	return nil
}

// matchesConfig checks if two configs are compatible for reuse
func (p *GPUIndexPool) matchesConfig(a, b types.GPUConfig) bool {
	// For now, match on dimension and device
	// More sophisticated matching could compare all fields
	return a.Dimension == b.Dimension && a.DeviceID == b.DeviceID
}

// isExpired checks if a pooled index has exceeded idle timeout
func (p *GPUIndexPool) isExpired(pi *pooledIndex) bool {
	if p.idleTimeout <= 0 {
		return false
	}
	return time.Since(pi.lastUsedAt) > p.idleTimeout
}

// Cleanup removes expired idle indexes
func (p *GPUIndexPool) Cleanup() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	removed := 0
	for e := p.idle.Front(); e != nil; {
		next := e.Next()
		pi := e.Value.(*pooledIndex)
		if p.isExpired(pi) {
			p.idle.Remove(e)
			if err := pi.index.Close(); err != nil {
				// Log error but continue cleanup
				fmt.Printf("GPU index close error: %v\n", err)
			}
			removed++
		}
		e = next
	}

	return removed
}

// Stats returns pool statistics
func (p *GPUIndexPool) Stats() GPUIndexPoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return GPUIndexPoolStats{
		Idle:          p.idle.Len(),
		Active:        p.active,
		Waiting:       p.waiting,
		TotalCreated:  p.totalCreated,
		TotalReused:   p.totalReused,
		MaxSize:       p.maxSize,
		MaxConcurrent: p.maxConcurrent,
	}
}

// UpdateMetrics updates Prometheus metrics for the pool
func (p *GPUIndexPool) UpdateMetrics() {
	stats := p.Stats()

	if metrics.GPUIndexPoolIdle != nil {
		metrics.GPUIndexPoolIdle.Set(float64(stats.Idle))
	}
	if metrics.GPUIndexPoolActive != nil {
		metrics.GPUIndexPoolActive.Set(float64(stats.Active))
	}
	if metrics.GPUIndexPoolTotalCreated != nil {
		// Counters are cumulative, so we set the total
		metrics.GPUIndexPoolTotalCreated.Add(float64(stats.TotalCreated))
	}
	if metrics.GPUIndexPoolTotalReused != nil {
		metrics.GPUIndexPoolTotalReused.Add(float64(stats.TotalReused))
	}
}

// GPUIndexPoolStats contains pool statistics
type GPUIndexPoolStats struct {
	Idle          int
	Active        int
	Waiting       int
	TotalCreated  int
	TotalReused   int
	MaxSize       int
	MaxConcurrent int
}

// Close closes the pool and all indexes
func (p *GPUIndexPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	// Close all idle indexes
	for e := p.idle.Front(); e != nil; e = e.Next() {
		pi := e.Value.(*pooledIndex)
		if err := pi.index.Close(); err != nil {
			// Log error but continue closing others
			fmt.Printf("GPU index close error: %v\n", err)
		}
	}
	p.idle.Init()

	return nil
}

// pooledIndexWrapper wraps a pooled index to intercept Close() calls
type pooledIndexWrapper struct {
	pool   *GPUIndexPool
	pooled *pooledIndex
}

// Add delegates to the wrapped index
func (w *pooledIndexWrapper) Add(ids []int64, vectors []float32) error {
	return w.pooled.index.Add(ids, vectors)
}

// Search delegates to the wrapped index
func (w *pooledIndexWrapper) Search(vector []float32, k int) ([]int64, []float32, error) {
	return w.pooled.index.Search(vector, k)
}

func (w *pooledIndexWrapper) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	return w.pooled.index.SearchPQ(lookupTable, m, k)
}

// Close returns the index to the pool instead of closing it
func (w *pooledIndexWrapper) Close() error {
	return w.pool.ReturnGPUIndex(w)
}

// Backend delegates to the wrapped index
func (w *pooledIndexWrapper) Backend() types.GPUBackend {
	return w.pooled.index.Backend()
}

// DeviceID delegates to the wrapped index
func (w *pooledIndexWrapper) DeviceID() int {
	return w.pooled.index.DeviceID()
}

// GetDeviceInfo delegates to the wrapped index
func (w *pooledIndexWrapper) GetDeviceInfo() (*types.GPUInfo, error) {
	return w.pooled.index.GetDeviceInfo()
}

// GetMemoryInfo delegates to the wrapped index
func (w *pooledIndexWrapper) GetMemoryInfo() (total, free, used int64, err error) {
	return w.pooled.index.GetMemoryInfo()
}

// GetUtilization delegates to the wrapped index
func (w *pooledIndexWrapper) GetUtilization() (float32, error) {
	return w.pooled.index.GetUtilization()
}

// SearchBatch delegates to the wrapped index
func (w *pooledIndexWrapper) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	return w.pooled.index.SearchBatch(vectors, k)
}
