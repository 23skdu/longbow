package store

import (
	"errors"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

type RateLimitConfig struct {
	RequestsPerSecond float64
	Burst             int
	Enabled           bool
}

type namespaceRateLimiter struct {
	mu           sync.RWMutex
	requests     []int64
	windowStart  time.Time
	config       RateLimitConfig
	blocked      bool
	blockExpires time.Time
}

func newNamespaceRateLimiter(config RateLimitConfig) *namespaceRateLimiter {
	return &namespaceRateLimiter{
		config:      config,
		windowStart: time.Now(),
		requests:    make([]int64, 0),
	}
}

func (r *namespaceRateLimiter) Allow() bool {
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

func (r *namespaceRateLimiter) SetConfig(config RateLimitConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.config = config
}

func (r *namespaceRateLimiter) GetConfig() RateLimitConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

type RateLimiterManager struct {
	mu         sync.RWMutex
	limiters   map[string]*namespaceRateLimiter
	defaultCfg RateLimitConfig
}

func NewRateLimiterManager(defaultCfg RateLimitConfig) *RateLimiterManager {
	return &RateLimiterManager{
		defaultCfg: defaultCfg,
		limiters:   make(map[string]*namespaceRateLimiter),
	}
}

func (m *RateLimiterManager) GetLimiter(namespace string) *namespaceRateLimiter {
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

func (m *RateLimiterManager) SetLimit(namespace string, config RateLimitConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limiter, ok := m.limiters[namespace]; ok {
		limiter.SetConfig(config)
	} else {
		m.limiters[namespace] = newNamespaceRateLimiter(config)
	}
}

func (m *RateLimiterManager) RemoveLimit(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.limiters, namespace)
}

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
	ErrNamespaceRateLimited = errors.New("namespace rate limit exceeded")
)

func (vs *VectorStore) CheckNamespaceRateLimit(namespace string) error {
	if vs.rateLimiterManager == nil {
		return nil
	}

	if !vs.rateLimiterManager.Allow(namespace) {
		return ErrNamespaceRateLimited
	}
	return nil
}

func (vs *VectorStore) SetNamespaceRateLimit(namespace string, config RateLimitConfig) error {
	if vs.rateLimiterManager == nil {
		vs.rateLimiterManager = NewRateLimiterManager(RateLimitConfig{Enabled: false})
	}
	vs.rateLimiterManager.SetLimit(namespace, config)
	return nil
}

func (vs *VectorStore) GetNamespaceRateLimit(namespace string) (RateLimitConfig, error) {
	if vs.rateLimiterManager == nil {
		return RateLimitConfig{}, nil
	}
	return vs.rateLimiterManager.GetLimiter(namespace).GetConfig(), nil
}

