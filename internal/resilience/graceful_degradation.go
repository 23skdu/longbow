package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DegradationLevel represents a degradation level value.
type DegradationLevel int

const (
	// DegradationNone is a degradation none constant.
	DegradationNone DegradationLevel = iota
	// DegradationMinimal is a degradation minimal constant.
	DegradationMinimal
	// DegradationModerate is a degradation moderate constant.
	DegradationModerate
	// DegradationSevere is a degradation severe constant.
	DegradationSevere
	// DegradationCritical is a degradation critical constant.
	DegradationCritical
)

// FallbackFunc represents a fallback func value.
type FallbackFunc func(ctx context.Context) (interface{}, error)

// DegradationStrategy represents a degradation strategy value.
type DegradationStrategy struct {
	Name         string
	Level        DegradationLevel
	FallbackFunc FallbackFunc
	Enabled      bool
	LastUsed     time.Time
}

// GracefulDegradation represents a graceful degradation value.
type GracefulDegradation struct {
	strategies      map[string]*DegradationStrategy
	currentLevel    DegradationLevel
	healthChecks    map[string]func() error
	mu              sync.RWMutex
	degradedSince   time.Time
	lastLevelChange time.Time
}

// NewGracefulDegradation constructs a new GracefulDegradation.
func NewGracefulDegradation() *GracefulDegradation {
	return &GracefulDegradation{
		strategies:   make(map[string]*DegradationStrategy),
		healthChecks: make(map[string]func() error),
		currentLevel: DegradationNone,
	}
}

// AddStrategy runs the add strategy operation.
func (gd *GracefulDegradation) AddStrategy(name string, level DegradationLevel, fallback FallbackFunc, enabled bool) {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	gd.strategies[name] = &DegradationStrategy{
		Name:         name,
		Level:        level,
		FallbackFunc: fallback,
		Enabled:      enabled,
	}
}

// AddHealthCheck runs the add health check operation.
func (gd *GracefulDegradation) AddHealthCheck(name string, check func() error) {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	gd.healthChecks[name] = check
}

// Execute runs the execute operation.
func (gd *GracefulDegradation) Execute(ctx context.Context, primary func() (any, error), fallbackStrategy string) (any, error) {
	gd.mu.RLock()
	strategy, exists := gd.strategies[fallbackStrategy]
	if !exists || !strategy.Enabled {
		gd.mu.RUnlock()
		return primary()
	}

	currentLevel := gd.currentLevel
	gd.mu.RUnlock()

	if currentLevel >= strategy.Level {
		result, err := strategy.FallbackFunc(ctx)
		if err == nil {
			gd.recordFallbackUse(fallbackStrategy)
			return result, nil
		}
	}

	return primary()
}

// ExecuteWithGracefulDegradation runs the execute with graceful degradation operation.
func ExecuteWithGracefulDegradation[T any](gd *GracefulDegradation, ctx context.Context, primary func() (T, error), fallbackStrategy string) (T, error) {
	gd.mu.RLock()
	strategy, exists := gd.strategies[fallbackStrategy]
	if !exists || !strategy.Enabled {
		gd.mu.RUnlock()
		return primary()
	}

	currentLevel := gd.currentLevel
	gd.mu.RUnlock()

	if currentLevel >= strategy.Level {
		result, err := strategy.FallbackFunc(ctx)
		if err == nil {
			gd.recordFallbackUse(fallbackStrategy)
			var zero T
			if result != nil {
				if r, ok := result.(T); ok {
					return r, nil
				}
			}
			return zero, nil
		}
	}

	return primary()
}

// GetCurrentLevel returns the current level.
func (gd *GracefulDegradation) GetCurrentLevel() DegradationLevel {
	gd.mu.RLock()
	defer gd.mu.RUnlock()
	return gd.currentLevel
}

// SetLevel sets the level.
func (gd *GracefulDegradation) SetLevel(level DegradationLevel) {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	if gd.currentLevel != level {
		gd.currentLevel = level
		gd.lastLevelChange = time.Now()

		if level > DegradationNone && gd.degradedSince.IsZero() {
			gd.degradedSince = time.Now()
		} else if level == DegradationNone {
			gd.degradedSince = time.Time{}
		}
	}
}

// AssessHealth runs the assess health operation.
func (gd *GracefulDegradation) AssessHealth() DegradationLevel {
	gd.mu.RLock()
	healthChecks := gd.healthChecks
	gd.mu.RUnlock()

	if len(healthChecks) == 0 {
		return DegradationNone
	}

	total := len(healthChecks)
	failed := 0

	for _, check := range healthChecks {
		if err := check(); err != nil {
			failed++
		}
	}

	failureRate := float64(failed) / float64(total)

	switch {
	case failureRate == 0:
		return DegradationNone
	case failureRate <= 0.1:
		return DegradationMinimal
	case failureRate <= 0.3:
		return DegradationModerate
	case failureRate <= 0.6:
		return DegradationSevere
	default:
		return DegradationCritical
	}
}

// StartHealthMonitoring runs the start health monitoring operation.
func (gd *GracefulDegradation) StartHealthMonitoring(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			level := gd.AssessHealth()
			gd.SetLevel(level)
		}
	}
}

func (gd *GracefulDegradation) recordFallbackUse(strategyName string) {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	if strategy, exists := gd.strategies[strategyName]; exists {
		strategy.LastUsed = time.Now()
	}
}

// GetStats returns the stats.
func (gd *GracefulDegradation) GetStats() map[string]interface{} {
	gd.mu.RLock()
	defer gd.mu.RUnlock()

	stats := map[string]interface{}{
		"current_level":     gd.currentLevel,
		"degraded_since":    gd.degradedSince,
		"last_level_change": gd.lastLevelChange,
		"strategies":        make(map[string]interface{}),
		"health_checks":     len(gd.healthChecks),
	}

	strategies := stats["strategies"].(map[string]interface{})
	for name, strategy := range gd.strategies {
		strategies[name] = map[string]interface{}{
			"level":     strategy.Level,
			"enabled":   strategy.Enabled,
			"last_used": strategy.LastUsed,
		}
	}

	return stats
}

// FallbackCache represents a fallback cache value.
type FallbackCache struct {
	cache map[string]CacheEntry
	ttl   time.Duration
	mu    sync.RWMutex
}

// CacheEntry represents a cache entry value.
type CacheEntry struct {
	Value      interface{}
	Expiry     time.Time
	Error      error
	IsFallback bool
}

// NewFallbackCache constructs a new FallbackCache.
func NewFallbackCache(ttl time.Duration) *FallbackCache {
	return &FallbackCache{
		cache: make(map[string]CacheEntry),
		ttl:   ttl,
	}
}

// Get returns the .
func (fc *FallbackCache) Get(key string) (value interface{}, found bool, err error) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	entry, exists := fc.cache[key]
	if !exists || time.Now().After(entry.Expiry) {
		if exists {
			delete(fc.cache, key)
		}
		return nil, false, nil
	}

	value = entry.Value
	found = entry.IsFallback
	err = entry.Error
	return
}

// Set sets the .
func (fc *FallbackCache) Set(key string, value interface{}, isFallback bool, err error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.cache[key] = CacheEntry{
		Value:      value,
		Expiry:     time.Now().Add(fc.ttl),
		Error:      err,
		IsFallback: isFallback,
	}
}

// Clear runs the clear operation.
func (fc *FallbackCache) Clear() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.cache = make(map[string]CacheEntry)
}

// Size runs the size operation.
func (fc *FallbackCache) Size() int {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	return len(fc.cache)
}

// Bulkhead represents a bulkhead value.
type Bulkhead struct {
	semaphore      chan struct{}
	name           string
	maxConcurrency int
	mu             sync.RWMutex
}

// NewBulkhead constructs a new Bulkhead.
func NewBulkhead(name string, maxConcurrency int) *Bulkhead {
	return &Bulkhead{
		semaphore:      make(chan struct{}, maxConcurrency),
		name:           name,
		maxConcurrency: maxConcurrency,
	}
}

// Execute runs the execute operation.
func (b *Bulkhead) Execute(fn func() (interface{}, error)) (interface{}, error) {
	select {
	case b.semaphore <- struct{}{}:
		defer func() { <-b.semaphore }()
		return fn()
	default:
		return nil, fmt.Errorf("bulkhead '%s' is full, max concurrency (%d) reached", b.name, b.maxConcurrency)
	}
}

// ExecuteWithBulkhead runs the execute with bulkhead operation.
func ExecuteWithBulkhead[T any](b *Bulkhead, fn func() (T, error)) (T, error) {
	select {
	case b.semaphore <- struct{}{}:
		defer func() { <-b.semaphore }()
		return fn()
	default:
		var zero T
		return zero, fmt.Errorf("bulkhead '%s' is full, max concurrency (%d) reached", b.name, b.maxConcurrency)
	}
}

// ExecuteWithContext runs the execute with context operation.
func (b *Bulkhead) ExecuteWithContext(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	select {
	case b.semaphore <- struct{}{}:
		defer func() { <-b.semaphore }()
		return fn(ctx)
	case <-ctx.Done():
		return nil, fmt.Errorf("bulkhead '%s' context cancelled: %w", b.name, ctx.Err())
	}
}

// ExecuteWithBulkheadAndContext runs the execute with bulkhead and context operation.
func ExecuteWithBulkheadAndContext[T any](b *Bulkhead, ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	select {
	case b.semaphore <- struct{}{}:
		defer func() { <-b.semaphore }()
		return fn(ctx)
	case <-ctx.Done():
		var zero T
		return zero, fmt.Errorf("bulkhead '%s' context cancelled: %w", b.name, ctx.Err())
	}
}

// AvailableSlots runs the available slots operation.
func (b *Bulkhead) AvailableSlots() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.maxConcurrency - len(b.semaphore)
}

// Name runs the name operation.
func (b *Bulkhead) Name() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.name
}

// BulkheadGroup represents a bulkhead group value.
type BulkheadGroup struct {
	bulkheads map[string]*Bulkhead
	mu        sync.RWMutex
}

// NewBulkheadGroup constructs a new BulkheadGroup.
func NewBulkheadGroup() *BulkheadGroup {
	return &BulkheadGroup{
		bulkheads: make(map[string]*Bulkhead),
	}
}

// GetBulkhead returns the bulkhead.
func (bg *BulkheadGroup) GetBulkhead(name string, maxConcurrency int) *Bulkhead {
	bg.mu.RLock()
	bulkhead, exists := bg.bulkheads[name]
	bg.mu.RUnlock()

	if exists {
		return bulkhead
	}

	bg.mu.Lock()
	defer bg.mu.Unlock()

	if bulkhead, exists := bg.bulkheads[name]; exists {
		return bulkhead
	}

	bulkhead = NewBulkhead(name, maxConcurrency)
	bg.bulkheads[name] = bulkhead

	return bulkhead
}

// Execute runs the execute operation.
func (bg *BulkheadGroup) Execute(name string, maxConcurrency int, fn func() (any, error)) (any, error) {
	bulkhead := bg.GetBulkhead(name, maxConcurrency)
	return bulkhead.Execute(fn)
}

// ExecuteWithBulkheadGroup runs the execute with bulkhead group operation.
func ExecuteWithBulkheadGroup[T any](bg *BulkheadGroup, name string, maxConcurrency int, fn func() (T, error)) (T, error) {
	bulkhead := bg.GetBulkhead(name, maxConcurrency)
	return ExecuteWithBulkhead(bulkhead, fn)
}

// RemoveBulkhead runs the remove bulkhead operation.
func (bg *BulkheadGroup) RemoveBulkhead(name string) {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	delete(bg.bulkheads, name)
}
