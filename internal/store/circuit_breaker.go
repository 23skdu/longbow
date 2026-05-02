package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// CircuitState represents the current state of a circuit breaker.
type CircuitState int32

const (
	// CircuitClosed indicates the circuit is closed and requests are allowed.
	CircuitClosed CircuitState = iota
	// CircuitOpen indicates the circuit is open and requests are rejected.
	CircuitOpen
	// CircuitHalfOpen indicates the circuit is testing recovery.
	CircuitHalfOpen
)

// String returns the string representation of the CircuitState.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when circuit breaker rejects request
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreakerConfig holds configuration parameters for the circuit breaker.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of failures before the circuit opens.
	FailureThreshold int
	// SuccessThreshold is the number of successes in half-open state before the circuit closes.
	SuccessThreshold int
	// Timeout is the duration the circuit stays open before transitioning to half-open.
	Timeout time.Duration
}

// DefaultCircuitBreakerConfig returns a configuration with sensible default values.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
	}
}

// CircuitBreakerStats holds operational statistics for a circuit breaker.
type CircuitBreakerStats struct {
	Successes    int64
	Failures     int64
	Rejections   int64
	State        CircuitState
	LastFailure  time.Time
	LastSuccess  time.Time
	StateChanges int64
}

// CircuitBreaker implements the circuit breaker pattern to prevent cascading failures.
type CircuitBreaker struct {
	config CircuitBreakerConfig

	state atomic.Int32

	// Counters for circuit logic (may be reset)
	consecutiveFailures atomic.Int64
	halfOpenSuccesses   atomic.Int64

	// Counters for stats (never reset except by Reset())
	totalSuccesses atomic.Int64
	totalFailures  atomic.Int64
	rejectionCount atomic.Int64
	stateChanges   atomic.Int64

	lastFailure time.Time
	lastSuccess time.Time
	openedAt    time.Time
	timeMu      sync.RWMutex
}

// NewCircuitBreaker creates a new CircuitBreaker with the given configuration.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	cb := &CircuitBreaker{
		config: config,
	}
	cb.state.Store(int32(CircuitClosed))
	return cb
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() CircuitState {
	current := CircuitState(cb.state.Load())

	// Check if we should transition from open to half-open
	if current == CircuitOpen {
		cb.timeMu.RLock()
		openedAt := cb.openedAt
		cb.timeMu.RUnlock()

		if time.Since(openedAt) >= cb.config.Timeout {
			// Transition to half-open
			if cb.state.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen)) {
				cb.stateChanges.Add(1)
				cb.halfOpenSuccesses.Store(0) // Reset for half-open testing
				metrics.CircuitBreakerStateChanges.WithLabelValues("store", "open", "half-open").Inc()
			}
			return CircuitHalfOpen
		}
	}

	return current
}

// Allow checks if a request should be allowed through the circuit breaker.
func (cb *CircuitBreaker) Allow() bool {
	state := cb.State()

	switch state {
	case CircuitClosed:
		return true
	case CircuitHalfOpen:
		return true // Allow limited requests for testing
	case CircuitOpen:
		cb.rejectionCount.Add(1)
		metrics.CircuitBreakerRejections.Inc()
		return false
	}
	return false
}

// RecordSuccess increments the success count and potentially closes the circuit.
func (cb *CircuitBreaker) RecordSuccess() {
	// Get state FIRST to trigger any pending transitions
	state := cb.State()

	cb.totalSuccesses.Add(1)
	metrics.CircuitBreakerSuccesses.Inc()

	cb.timeMu.Lock()
	cb.lastSuccess = time.Now()
	cb.timeMu.Unlock()

	switch state {
	case CircuitHalfOpen:
		newCount := cb.halfOpenSuccesses.Add(1)
		// Check if we have enough successes to close
		if newCount >= int64(cb.config.SuccessThreshold) {
			if cb.state.CompareAndSwap(int32(CircuitHalfOpen), int32(CircuitClosed)) {
				cb.stateChanges.Add(1)
				cb.consecutiveFailures.Store(0) // Reset failure count
				metrics.CircuitBreakerStateChanges.WithLabelValues("store", "half-open", "closed").Inc()
			}
		}
	case CircuitClosed:
		// Reset consecutive failure count on success
		cb.consecutiveFailures.Store(0)
	}
}

// RecordFailure increments the failure count and potentially opens the circuit.
func (cb *CircuitBreaker) RecordFailure() {
	cb.totalFailures.Add(1)
	metrics.CircuitBreakerFailures.Inc()

	cb.timeMu.Lock()
	cb.lastFailure = time.Now()
	cb.timeMu.Unlock()

	state := cb.State()

	switch state {
	case CircuitHalfOpen:
		// Any failure in half-open returns to open
		if cb.state.CompareAndSwap(int32(CircuitHalfOpen), int32(CircuitOpen)) {
			cb.stateChanges.Add(1)
			cb.timeMu.Lock()
			cb.openedAt = time.Now()
			cb.timeMu.Unlock()
			metrics.CircuitBreakerStateChanges.WithLabelValues("store", "half-open", "open").Inc()
		}
	case CircuitClosed:
		newCount := cb.consecutiveFailures.Add(1)
		// Check if we exceed failure threshold
		if newCount >= int64(cb.config.FailureThreshold) {
			if cb.state.CompareAndSwap(int32(CircuitClosed), int32(CircuitOpen)) {
				cb.stateChanges.Add(1)
				cb.timeMu.Lock()
				cb.openedAt = time.Now()
				cb.timeMu.Unlock()
				metrics.CircuitBreakerStateChanges.WithLabelValues("store", "closed", "open").Inc()
			}
		}
	}
}

// Execute wraps a function call with circuit breaker protection.
func (cb *CircuitBreaker) Execute(fn func() (any, error)) (any, error) {
	if !cb.Allow() {
		return nil, ErrCircuitOpen
	}

	result, err := fn()
	if err != nil {
		cb.RecordFailure()
		return result, err
	}

	cb.RecordSuccess()
	return result, nil
}

// Stats returns a snapshot of the circuit breaker's operational statistics.
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	cb.timeMu.RLock()
	lastFailure := cb.lastFailure
	lastSuccess := cb.lastSuccess
	cb.timeMu.RUnlock()

	return CircuitBreakerStats{
		Successes:    cb.totalSuccesses.Load(),
		Failures:     cb.totalFailures.Load(),
		Rejections:   cb.rejectionCount.Load(),
		State:        cb.State(),
		LastFailure:  lastFailure,
		LastSuccess:  lastSuccess,
		StateChanges: cb.stateChanges.Load(),
	}
}

// Reset clears all counters and returns the circuit breaker to a closed state.
func (cb *CircuitBreaker) Reset() {
	cb.state.Store(int32(CircuitClosed))
	cb.consecutiveFailures.Store(0)
	cb.halfOpenSuccesses.Store(0)
	cb.totalSuccesses.Store(0)
	cb.totalFailures.Store(0)
	cb.rejectionCount.Store(0)
	cb.stateChanges.Store(0)

	cb.timeMu.Lock()
	cb.lastFailure = time.Time{}
	cb.lastSuccess = time.Time{}
	cb.openedAt = time.Time{}
	cb.timeMu.Unlock()
}

// =============================================================================
// CircuitBreakerRegistry
// =============================================================================

// CircuitBreakerRegistry manages a collection of circuit breakers identified by keys.
type CircuitBreakerRegistry struct {
	breakers sync.Map // map[string]*CircuitBreaker
	config   CircuitBreakerConfig
}

// NewCircuitBreakerRegistry creates a new registry with a default configuration.
func NewCircuitBreakerRegistry(config CircuitBreakerConfig) *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{
		config: config,
	}
}

// GetOrCreate retrieves an existing circuit breaker or creates one if it doesn't exist.
func (r *CircuitBreakerRegistry) GetOrCreate(key string) *CircuitBreaker {
	if value, ok := r.breakers.Load(key); ok {
		return value.(*CircuitBreaker)
	}

	cb := NewCircuitBreaker(r.config)
	actual, _ := r.breakers.LoadOrStore(key, cb)
	return actual.(*CircuitBreaker)
}

// Reset resets the state of a specific circuit breaker in the registry.
func (r *CircuitBreakerRegistry) Reset(key string) {
	if value, ok := r.breakers.Load(key); ok {
		value.(*CircuitBreaker).Reset()
	}
}

// ResetAll resets all circuit breakers managed by the registry.
func (r *CircuitBreakerRegistry) ResetAll() {
	r.breakers.Range(func(_, value any) bool {
		value.(*CircuitBreaker).Reset()
		return true
	})
}
