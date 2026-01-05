package breaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:        "test",
		ReadyToTrip: func(counts Counts) bool { return counts.ConsecutiveFailures >= 2 },
		Timeout:     100 * time.Millisecond,
	})

	// Initial State: Closed
	assert.Equal(t, StateClosed, cb.State())
	assert.True(t, cb.Allow())

	// Failure 1
	_, _ = cb.Execute(func() (interface{}, error) { return nil, assert.AnError })
	assert.Equal(t, StateClosed, cb.State())

	// Failure 2 (Trips)
	_, _ = cb.Execute(func() (interface{}, error) { return nil, assert.AnError })
	assert.Equal(t, StateOpen, cb.State())
	assert.False(t, cb.Allow())

	// Wait for Timeout
	time.Sleep(150 * time.Millisecond)

	// Should transition to Half-Open on next check
	assert.Equal(t, StateHalfOpen, cb.State())
	assert.True(t, cb.Allow())

	// Success in Half-Open -> Closed
	_, _ = cb.Execute(func() (interface{}, error) { return "ok", nil })
	assert.Equal(t, StateClosed, cb.State())
}

func TestCircuitBreaker_HalfOpenMaxRequests(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:        "test",
		MaxRequests: 1,
		ReadyToTrip: func(counts Counts) bool { return true },
		Timeout:     10 * time.Millisecond,
	})

	// Trip it
	_, _ = cb.Execute(func() (interface{}, error) { return nil, assert.AnError })
	assert.Equal(t, StateOpen, cb.State())

	// Wait for timeout
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, StateHalfOpen, cb.State())

	// First request allowed (MaxRequests=1)
	assert.True(t, cb.Allow())

	// Simulate using the quota without executing (manually increment for test)
	cb.mutex.Lock()
	cb.counts.Requests = 1
	cb.mutex.Unlock()

	// Second request denied
	assert.False(t, cb.Allow())
}
