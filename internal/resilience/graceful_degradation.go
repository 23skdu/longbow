package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type DegradationLevel int

const (
	DegradationNone DegradationLevel = iota
	DegradationMinimal
	DegradationModerate
	DegradationSevere
	DegradationCritical
)

type FallbackFunc func(ctx context.Context) (interface{}, error)

type DegradationStrategy struct {
	Name         string
	Level        DegradationLevel
	FallbackFunc FallbackFunc
	Enabled      bool
	LastUsed     time.Time
}

type GracefulDegradation struct {
	strategies      map[string]*DegradationStrategy
	currentLevel    DegradationLevel
	healthChecks    map[string]func() error
	mu              sync.RWMutex
	degradedSince   time.Time
	lastLevelChange time.Time
}

func NewGracefulDegradation() *GracefulDegradation {
	return &GracefulDegradation{
		strategies:   make(map[string]*DegradationStrategy),
		healthChecks: make(map[string]func() error),
		currentLevel: DegradationNone,
	}
}

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

func (gd *GracefulDegradation) AddHealthCheck(name string, check func() error) {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	gd.healthChecks[name] = check
}

func (gd *GracefulDegradation) Execute(ctx context.Context, primary func() (interface{}, error), fallbackStrategy string) (interface{}, error) {
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

func (gd *GracefulDegradation) GetCurrentLevel() DegradationLevel {
	gd.mu.RLock()
	defer gd.mu.RUnlock()
	return gd.currentLevel
}

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

type FallbackCache struct {
	cache map[string]CacheEntry
	ttl   time.Duration
	mu    sync.RWMutex
}

type CacheEntry struct {
	Value      interface{}
	Expiry     time.Time
	Error      error
	IsFallback bool
}

func NewFallbackCache(ttl time.Duration) *FallbackCache {
	return &FallbackCache{
		cache: make(map[string]CacheEntry),
		ttl:   ttl,
	}
}

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

func (fc *FallbackCache) Clear() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.cache = make(map[string]CacheEntry)
}

func (fc *FallbackCache) Size() int {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	return len(fc.cache)
}

type Bulkhead struct {
	semaphore      chan struct{}
	name           string
	maxConcurrency int
	mu             sync.RWMutex
}

func NewBulkhead(name string, maxConcurrency int) *Bulkhead {
	return &Bulkhead{
		semaphore:      make(chan struct{}, maxConcurrency),
		name:           name,
		maxConcurrency: maxConcurrency,
	}
}

func (b *Bulkhead) Execute(fn func() (interface{}, error)) (interface{}, error) {
	select {
	case b.semaphore <- struct{}{}:
		defer func() { <-b.semaphore }()
		return fn()
	default:
		return nil, fmt.Errorf("bulkhead '%s' is full, max concurrency (%d) reached", b.name, b.maxConcurrency)
	}
}

func (b *Bulkhead) ExecuteWithContext(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	select {
	case b.semaphore <- struct{}{}:
		defer func() { <-b.semaphore }()
		return fn(ctx)
	case <-ctx.Done():
		return nil, fmt.Errorf("bulkhead '%s' context cancelled: %w", b.name, ctx.Err())
	}
}

func (b *Bulkhead) AvailableSlots() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.maxConcurrency - len(b.semaphore)
}

func (b *Bulkhead) Name() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.name
}

type BulkheadGroup struct {
	bulkheads map[string]*Bulkhead
	mu        sync.RWMutex
}

func NewBulkheadGroup() *BulkheadGroup {
	return &BulkheadGroup{
		bulkheads: make(map[string]*Bulkhead),
	}
}

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

func (bg *BulkheadGroup) Execute(name string, maxConcurrency int, fn func() (interface{}, error)) (interface{}, error) {
	bulkhead := bg.GetBulkhead(name, maxConcurrency)
	return bulkhead.Execute(fn)
}

func (bg *BulkheadGroup) RemoveBulkhead(name string) {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	delete(bg.bulkheads, name)
}
