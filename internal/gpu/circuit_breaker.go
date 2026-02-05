package gpu

import (
	"fmt"
	"sync"
	"time"
)

// CircuitState represents the state of the circuit breaker
type CircuitState int

const (
	// CircuitClosed means the circuit is closed and requests are allowed
	CircuitClosed CircuitState = iota
	// CircuitOpen means the circuit is open and requests are blocked
	CircuitOpen
	// CircuitHalfOpen means the circuit is testing if the service has recovered
	CircuitHalfOpen
)

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

// CircuitBreakerConfig configures the circuit breaker
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before opening the circuit
	FailureThreshold int
	// SuccessThreshold is the number of consecutive successes in half-open state before closing
	SuccessThreshold int
	// Timeout is the duration the circuit stays open before transitioning to half-open
	Timeout time.Duration
}

// DefaultCircuitBreakerConfig returns a default configuration
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          30 * time.Second,
	}
}

// CircuitBreaker implements the circuit breaker pattern for GPU operations
type CircuitBreaker struct {
	config CircuitBreakerConfig

	mu              sync.RWMutex
	state           CircuitState
	failures        int
	successes       int
	lastFailure     time.Time
	lastStateChange time.Time
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config:          config,
		state:           CircuitClosed,
		lastStateChange: time.Now(),
	}
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Allow checks if a request should be allowed through
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if timeout has passed, transition to half-open
		if time.Since(cb.lastFailure) > cb.config.Timeout {
			cb.state = CircuitHalfOpen
			cb.successes = 0
			cb.lastStateChange = time.Now()
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return false
	}
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		// Reset failure count on success
		cb.failures = 0
	case CircuitHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.successes = 0
			cb.lastStateChange = time.Now()
		}
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitClosed:
		cb.failures++
		if cb.failures >= cb.config.FailureThreshold {
			cb.state = CircuitOpen
			cb.lastStateChange = time.Now()
		}
	case CircuitHalfOpen:
		// Immediately open on failure in half-open state
		cb.state = CircuitOpen
		cb.failures = cb.config.FailureThreshold
		cb.successes = 0
		cb.lastStateChange = time.Now()
	}
}

// Stats returns current circuit breaker statistics
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return CircuitBreakerStats{
		State:         cb.state.String(),
		Failures:      cb.failures,
		Successes:     cb.successes,
		LastFailure:   cb.lastFailure,
		StateDuration: time.Since(cb.lastStateChange),
	}
}

// CircuitBreakerStats holds circuit breaker statistics
type CircuitBreakerStats struct {
	State         string
	Failures      int
	Successes     int
	LastFailure   time.Time
	StateDuration time.Duration
}

// String returns a string representation of the stats
func (s CircuitBreakerStats) String() string {
	return fmt.Sprintf("CircuitBreaker{state=%s, failures=%d, successes=%d, last_failure=%v, state_duration=%v}",
		s.State, s.Failures, s.Successes, s.LastFailure, s.StateDuration)
}
