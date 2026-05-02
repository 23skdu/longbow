package store

import (
	"errors"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// RateLimitConfig defines the configuration for namespace-level rate limiting.
type RateLimitConfig struct {
	// RequestsPerSecond is the maximum number of requests allowed per second.
	RequestsPerSecond float64
	// Burst is the maximum number of requests that can be handled at once.
	Burst             int
	// Enabled indicates whether rate limiting is active.
	Enabled           bool
}

// NamespaceRateLimiter implements rate limiting for a specific namespace.
type NamespaceRateLimiter struct {
	mu           sync.RWMutex
	requests     []int64
	windowStart  time.Time
	config       RateLimitConfig
	blocked      bool
	blockExpires time.Time
}

func newNamespaceRateLimiter(config RateLimitConfig) *NamespaceRateLimiter {
	return &NamespaceRateLimiter{
		config:      config,
		windowStart: time.Now(),
		requests:    make([]int64, 0),
	}
}

// Allow returns true if a request is allowed under the current rate limit.
func (r *NamespaceRateLimiter) Allow() bool {
	if !r.config.Enabled {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.blocked {
		if time.Now().After(r.blockExpires) {
			r.blocked = false
			r.requests = r.requests[:0]
			r.windowStart = time.Now()
		} else {
			metrics.RecordNamespaceRateLimitHit("unknown")
			return false
		}
	}

	now := time.Now()
	windowDuration := time.Second

	if now.Sub(r.windowStart) >= windowDuration {
		r.requests = r.requests[:0]
		r.windowStart = now
	}

	currentRate := float64(len(r.requests)) / now.Sub(r.windowStart).Seconds()
	if currentRate > r.config.RequestsPerSecond {
		r.blocked = true
		r.blockExpires = now.Add(10 * time.Second)
		metrics.RecordNamespaceRateLimitHit("unknown")
		return false
	}

	r.requests = append(r.requests, now.UnixNano())
	return true
}

// SetConfig updates the rate limit configuration for the namespace.
func (r *NamespaceRateLimiter) SetConfig(config RateLimitConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.config = config
}

// GetConfig returns the current rate limit configuration for the namespace.
func (r *NamespaceRateLimiter) GetConfig() RateLimitConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

// RateLimiterManager coordinates rate limiters across multiple namespaces.
type RateLimiterManager struct {
	mu         sync.RWMutex
	limiters   map[string]*NamespaceRateLimiter
	defaultCfg RateLimitConfig
}

// NewRateLimiterManager creates a new RateLimiterManager with the given default configuration.
func NewRateLimiterManager(defaultCfg RateLimitConfig) *RateLimiterManager {
	return &RateLimiterManager{
		defaultCfg: defaultCfg,
		limiters:   make(map[string]*NamespaceRateLimiter),
	}
}

// GetLimiter returns the rate limiter for the specified namespace, creating one if it doesn't exist.
func (m *RateLimiterManager) GetLimiter(namespace string) *NamespaceRateLimiter {
	m.mu.RLock()
	limiter, ok := m.limiters[namespace]
	m.mu.RUnlock()

	if ok {
		return limiter
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if limiter, ok = m.limiters[namespace]; ok {
		return limiter
	}

	limiter = newNamespaceRateLimiter(m.defaultCfg)
	m.limiters[namespace] = limiter
	return limiter
}

// SetLimit configures the rate limit for a specific namespace.
func (m *RateLimiterManager) SetLimit(namespace string, config RateLimitConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limiter, ok := m.limiters[namespace]; ok {
		limiter.SetConfig(config)
	} else {
		m.limiters[namespace] = newNamespaceRateLimiter(config)
	}
}

// RemoveLimit deletes the rate limiter for a specific namespace.
func (m *RateLimiterManager) RemoveLimit(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.limiters, namespace)
}

// Allow checks if a request should be allowed for the given namespace.
func (m *RateLimiterManager) Allow(namespace string) bool {
	limiter := m.GetLimiter(namespace)
	allowed := limiter.Allow()

	ns := namespace
	if !allowed {
		metrics.RecordNamespaceRateLimitHit(ns)
	}
	return allowed
}

var (
	// ErrNamespaceRateLimited is returned when a namespace has exceeded its request quota.
	ErrNamespaceRateLimited = errors.New("namespace rate limit exceeded")
)

// CheckNamespaceRateLimit is a helper for VectorStore to verify rate limits.
func (vs *VectorStore) CheckNamespaceRateLimit(namespace string) error {
	if vs.rateLimiterManager == nil {
		return nil
	}

	if !vs.rateLimiterManager.Allow(namespace) {
		return ErrNamespaceRateLimited
	}
	return nil
}

// SetNamespaceRateLimit configures rate limiting for a namespace in the VectorStore.
func (vs *VectorStore) SetNamespaceRateLimit(namespace string, config RateLimitConfig) error {
	if vs.rateLimiterManager == nil {
		vs.rateLimiterManager = NewRateLimiterManager(RateLimitConfig{Enabled: false})
	}
	vs.rateLimiterManager.SetLimit(namespace, config)
	return nil
}

// GetNamespaceRateLimit retrieves the current rate limit configuration for a namespace.
func (vs *VectorStore) GetNamespaceRateLimit(namespace string) (RateLimitConfig, error) {
	if vs.rateLimiterManager == nil {
		return RateLimitConfig{}, nil
	}
	return vs.rateLimiterManager.GetLimiter(namespace).GetConfig(), nil
}

