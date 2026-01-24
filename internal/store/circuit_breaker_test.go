package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Circuit Breaker Tests - TDD Red Phase
// =============================================================================

func TestCircuitBreaker_InitialStateClosed(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	if cb.State() != CircuitClosed {
		t.Errorf("expected initial state Closed, got %v", cb.State())
	}
}

func TestCircuitBreaker_OpensAfterFailureThreshold(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)

	// Record failures up to threshold
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.State() != CircuitOpen {
		t.Errorf("expected state Open after %d failures, got %v", 3, cb.State())
	}
}

func TestCircuitBreaker_RejectsWhenOpen(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		Timeout:          1 * time.Second,
	}
	cb := NewCircuitBreaker(cfg)
	cb.RecordFailure() // Opens circuit

	allowed := cb.Allow()
	if allowed {
		t.Error("expected request to be rejected when circuit is open")
	}
}

func TestCircuitBreaker_TransitionsToHalfOpenAfterTimeout(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		Timeout:          50 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)
	cb.RecordFailure() // Opens circuit

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	if cb.State() != CircuitHalfOpen {
		t.Errorf("expected state HalfOpen after timeout, got %v", cb.State())
	}
}

func TestCircuitBreaker_ClosesAfterSuccessThreshold(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)
	cb.RecordFailure() // Opens circuit

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)

	// Record successes
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != CircuitClosed {
		t.Errorf("expected state Closed after successes, got %v", cb.State())
	}
}

func TestCircuitBreaker_ReOpensOnFailureInHalfOpen(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)
	cb.RecordFailure() // Opens circuit

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)

	// Fail in half-open
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Errorf("expected state Open after failure in half-open, got %v", cb.State())
	}
}

func TestCircuitBreaker_Execute_Success(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	result, err := cb.Execute(func() (any, error) {
		return "success", nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if result != "success" {
		t.Errorf("expected 'success', got %v", result)
	}
}

func TestCircuitBreaker_Execute_RejectsWhenOpen(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		Timeout:          1 * time.Second,
	}
	cb := NewCircuitBreaker(cfg)
	cb.RecordFailure() // Open circuit

	_, err := cb.Execute(func() (any, error) {
		return "should not run", nil
	})

	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_Metrics(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)

	// Record some operations
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordFailure()

	stats := cb.Stats()
	if stats.Successes != 2 {
		t.Errorf("expected 2 successes, got %d", stats.Successes)
	}
	if stats.Failures != 1 {
		t.Errorf("expected 1 failure, got %d", stats.Failures)
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 100,
		SuccessThreshold: 10,
		Timeout:          100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)

	var wg sync.WaitGroup
	var successCount, failureCount atomic.Int64

	// Concurrent operations
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if n%2 == 0 {
				cb.RecordSuccess()
				successCount.Add(1)
			} else {
				cb.RecordFailure()
				failureCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	stats := cb.Stats()
	if stats.Successes != successCount.Load() {
		t.Errorf("success count mismatch: expected %d, got %d", successCount.Load(), stats.Successes)
	}
	if stats.Failures != failureCount.Load() {
		t.Errorf("failure count mismatch: expected %d, got %d", failureCount.Load(), stats.Failures)
	}
}

func TestCircuitBreakerRegistry_GetOrCreate(t *testing.T) {
	reg := NewCircuitBreakerRegistry(DefaultCircuitBreakerConfig())

	cb1 := reg.GetOrCreate("peer1")
	cb2 := reg.GetOrCreate("peer1")
	cb3 := reg.GetOrCreate("peer2")

	if cb1 != cb2 {
		t.Error("expected same circuit breaker for same peer")
	}
	if cb1 == cb3 {
		t.Error("expected different circuit breakers for different peers")
	}
}

func TestCircuitBreakerRegistry_Reset(t *testing.T) {
	reg := NewCircuitBreakerRegistry(DefaultCircuitBreakerConfig())

	cb := reg.GetOrCreate("peer1")
	for i := 0; i < 5; i++ {
		cb.RecordFailure()
	}

	if cb.State() != CircuitOpen {
		t.Error("expected circuit to be open")
	}

	reg.Reset("peer1")
	cb = reg.GetOrCreate("peer1")

	if cb.State() != CircuitClosed {
		t.Errorf("expected circuit to be closed after reset, got %v", cb.State())
	}
}
