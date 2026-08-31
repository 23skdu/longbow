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
	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	assert.Equal(t, StateClosed, cb.State())

	// Failure 2 (Trips)
	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	assert.Equal(t, StateOpen, cb.State())
	assert.False(t, cb.Allow())

	// Wait for Timeout
	time.Sleep(150 * time.Millisecond)

	// Should transition to Half-Open on next check
	assert.Equal(t, StateHalfOpen, cb.State())
	assert.True(t, cb.Allow())

	// Success in Half-Open -> Closed
	_, _ = cb.Execute(func() (any, error) { return "ok", nil })
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
	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
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

func TestCircuitBreaker_Name(t *testing.T) {
	cb := NewCircuitBreaker(Settings{Name: "my-breaker"})
	assert.Equal(t, "my-breaker", cb.Name())
}

func TestError_Error(t *testing.T) {
	err := &Error{Msg: "circuit breaker is open"}
	assert.Equal(t, "circuit breaker is open", err.Error())

	assert.Equal(t, "circuit breaker is open", ErrOpenState.Error())
}

func TestCircuitBreaker_Execute_InOpenState(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:        "test",
		ReadyToTrip: func(counts Counts) bool { return counts.ConsecutiveFailures >= 1 },
		Timeout:     10 * time.Second,
	})

	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	assert.Equal(t, StateOpen, cb.State())

	_, err := cb.Execute(func() (any, error) { return nil, nil })
	assert.ErrorIs(t, err, ErrOpenState)
}

func TestCircuitBreaker_HalfOpen_FailureTripsOpen(t *testing.T) {
	cb := NewCircuitBreaker(Settings{
		Name:        "test",
		ReadyToTrip: func(counts Counts) bool { return counts.ConsecutiveFailures >= 1 },
		Timeout:     50 * time.Millisecond,
	})

	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	assert.Equal(t, StateOpen, cb.State())

	time.Sleep(70 * time.Millisecond)
	assert.Equal(t, StateHalfOpen, cb.State())

	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	assert.Equal(t, StateOpen, cb.State())
}

func TestCircuitBreaker_OnStateChangeCallback(t *testing.T) {
	type transition struct {
		name string
		from State
		to   State
	}
	var transitions []transition

	cb := NewCircuitBreaker(Settings{
		Name:        "test",
		ReadyToTrip: func(counts Counts) bool { return counts.ConsecutiveFailures >= 1 },
		Timeout:     50 * time.Millisecond,
		OnStateChange: func(name string, from, to State) {
			transitions = append(transitions, transition{name, from, to})
		},
	})

	// Closed -> Open
	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	assert.Len(t, transitions, 1)
	assert.Equal(t, transition{"test", StateClosed, StateOpen}, transitions[0])

	// Open -> HalfOpen
	time.Sleep(70 * time.Millisecond)
	_ = cb.State()
	assert.Len(t, transitions, 2)
	assert.Equal(t, transition{"test", StateOpen, StateHalfOpen}, transitions[1])

	// HalfOpen -> Closed
	_, _ = cb.Execute(func() (any, error) { return "ok", nil })
	assert.Len(t, transitions, 3)
	assert.Equal(t, transition{"test", StateHalfOpen, StateClosed}, transitions[2])
}

func TestNewCircuitBreaker_DefaultSettings(t *testing.T) {
	cb := NewCircuitBreaker(Settings{})

	assert.Equal(t, StateClosed, cb.State())

	for i := 0; i < 5; i++ {
		_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	}
	assert.Equal(t, StateClosed, cb.State(), "should stay closed after 5 failures (ConsecutiveFailures > 5)")

	_, _ = cb.Execute(func() (any, error) { return nil, assert.AnError })
	assert.Equal(t, StateOpen, cb.State(), "should trip to open after 6 failures")
}
