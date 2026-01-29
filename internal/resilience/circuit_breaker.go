package resilience

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type CircuitState int

const (
	StateClosed CircuitState = iota
	StateHalfOpen
	StateOpen
)

type CircuitBreakerMetrics struct {
	Requests            uint64
	Successes           uint64
	Failures            uint64
	Timeouts            uint64
	TotalBreakTime      time.Duration
	LastFailureTime     time.Time
	ConsecutiveFailures uint64
}

type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	readyToTrip   func(counts CircuitBreakerMetrics) bool
	onStateChange func(name string, from, to CircuitState)

	state      CircuitState
	generation uint64
	counts     CircuitBreakerMetrics
	expiry     time.Time
	mu         sync.Mutex
}

type CircuitBreakerSettings struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts CircuitBreakerMetrics) bool
	OnStateChange func(name string, from, to CircuitState)
}

func NewCircuitBreaker(settings CircuitBreakerSettings) *CircuitBreaker {
	if settings.MaxRequests == 0 {
		settings.MaxRequests = 1
	}
	if settings.Interval == 0 {
		settings.Interval = 60 * time.Second
	}
	if settings.Timeout == 0 {
		settings.Timeout = 60 * time.Second
	}
	if settings.ReadyToTrip == nil {
		settings.ReadyToTrip = DefaultReadyToTrip
	}

	return &CircuitBreaker{
		name:          settings.Name,
		maxRequests:   settings.MaxRequests,
		interval:      settings.Interval,
		timeout:       settings.Timeout,
		readyToTrip:   settings.ReadyToTrip,
		onStateChange: settings.OnStateChange,
		state:         StateClosed,
	}
}

func DefaultReadyToTrip(counts CircuitBreakerMetrics) bool {
	return counts.ConsecutiveFailures > 5 ||
		(counts.Failures > 10 && float64(counts.Failures)/float64(counts.Requests) > 0.6)
}

func (cb *CircuitBreaker) Execute(ctx context.Context, req func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	result, err := req()
	cb.afterRequest(generation, err)
	return result, err
}

func (cb *CircuitBreaker) Name() string {
	return cb.name
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

func (cb *CircuitBreaker) Metrics() CircuitBreakerMetrics {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	return cb.counts
}

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, fmt.Errorf("circuit breaker '%s' is open", cb.name)
	}

	if state == StateHalfOpen && cb.counts.Requests >= uint64(cb.maxRequests) {
		return generation, fmt.Errorf("circuit breaker '%s' max requests reached in half-open state", cb.name)
	}

	atomic.AddUint64(&cb.counts.Requests, 1)
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(before uint64, err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if generation != before {
		return
	}

	if err != nil {
		cb.onFailure(state, now)
	} else {
		cb.onSuccess(state, now)
	}
}

func (cb *CircuitBreaker) onSuccess(state CircuitState, now time.Time) {
	atomic.AddUint64(&cb.counts.Successes, 1)
	atomic.StoreUint64(&cb.counts.ConsecutiveFailures, 0)

	if state == StateHalfOpen {
		cb.setState(StateClosed, now)
	}
}

func (cb *CircuitBreaker) onFailure(state CircuitState, now time.Time) {
	atomic.AddUint64(&cb.counts.Failures, 1)
	atomic.AddUint64(&cb.counts.ConsecutiveFailures, 1)
	cb.counts.LastFailureTime = now

	switch state {
	case StateClosed:
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	}
}

func (cb *CircuitBreaker) currentState(now time.Time) (state CircuitState, failures uint64) {
	switch cb.state {
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	state = cb.state
	failures = cb.generation
	return
}

func (cb *CircuitBreaker) setState(state CircuitState, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.Requests = 0
	cb.counts.Successes = 0
	cb.counts.Failures = 0
	cb.counts.Timeouts = 0

	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
		cb.counts.TotalBreakTime += cb.timeout
	default:
		cb.expiry = zero
	}
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateClosed, time.Now())
	cb.counts = CircuitBreakerMetrics{}
}

type CircuitBreakerGroup struct {
	breakers map[string]*CircuitBreaker
	mu       sync.RWMutex
	settings CircuitBreakerSettings
}

func NewCircuitBreakerGroup(settings CircuitBreakerSettings) *CircuitBreakerGroup {
	return &CircuitBreakerGroup{
		breakers: make(map[string]*CircuitBreaker),
		settings: settings,
	}
}

func (cbg *CircuitBreakerGroup) GetBreaker(name string) *CircuitBreaker {
	cbg.mu.RLock()
	breaker, exists := cbg.breakers[name]
	cbg.mu.RUnlock()

	if exists {
		return breaker
	}

	cbg.mu.Lock()
	defer cbg.mu.Unlock()

	if breaker, exists := cbg.breakers[name]; exists {
		return breaker
	}

	settings := cbg.settings
	settings.Name = name
	breaker = NewCircuitBreaker(settings)
	cbg.breakers[name] = breaker

	return breaker
}

func (cbg *CircuitBreakerGroup) Execute(ctx context.Context, name string, req func() (interface{}, error)) (interface{}, error) {
	breaker := cbg.GetBreaker(name)
	return breaker.Execute(ctx, req)
}

func (cbg *CircuitBreakerGroup) GetAllMetrics() map[string]CircuitBreakerMetrics {
	cbg.mu.RLock()
	defer cbg.mu.RUnlock()

	metrics := make(map[string]CircuitBreakerMetrics)
	for name, breaker := range cbg.breakers {
		metrics[name] = breaker.Metrics()
	}

	return metrics
}

func (cbg *CircuitBreakerGroup) ResetAll() {
	cbg.mu.Lock()
	defer cbg.mu.Unlock()

	for _, breaker := range cbg.breakers {
		breaker.Reset()
	}
}

func (cbg *CircuitBreakerGroup) RemoveBreaker(name string) {
	cbg.mu.Lock()
	defer cbg.mu.Unlock()

	delete(cbg.breakers, name)
}
