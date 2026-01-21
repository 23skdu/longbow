package breaker

import (
	"sync"
	"time"
)

// State represents the current state of the circuit breaker
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// Settings configures the CircuitBreaker
type Settings struct {
	Name          string
	MaxRequests   uint32        // Max requests in Half-Open state
	Interval      time.Duration // Cyclic period of the closed state to clear counts
	Timeout       time.Duration // Time to wait before switching from Open to Half-Open
	ReadyToTrip   func(counts Counts) bool
	OnStateChange func(name string, from State, to State)
}

// Counts holds the numbers of requests and their results
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

func (c *Counts) clear() {
	c.Requests = 0
	c.TotalSuccesses = 0
	c.TotalFailures = 0
	c.ConsecutiveSuccesses = 0
	c.ConsecutiveFailures = 0
}

// CircuitBreaker is a state machine to prevent cascading failures
type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	readyToTrip   func(counts Counts) bool
	onStateChange func(name string, from State, to State)

	mutex      sync.Mutex
	state      State
	generation uint64
	counts     Counts
	expiry     time.Time
}

// NewCircuitBreaker creates a new CircuitBreaker
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:          st.Name,
		maxRequests:   st.MaxRequests,
		interval:      st.Interval,
		timeout:       st.Timeout,
		readyToTrip:   st.ReadyToTrip,
		onStateChange: st.OnStateChange,
	}

	if cb.maxRequests == 0 {
		cb.maxRequests = 1
	}

	if cb.interval == 0 {
		cb.interval = time.Duration(0) // No interval clearing
	}

	if cb.timeout == 0 {
		cb.timeout = 60 * time.Second
	}

	if cb.readyToTrip == nil {
		cb.readyToTrip = func(counts Counts) bool {
			return counts.ConsecutiveFailures > 5
		}
	}

	cb.toNewGeneration(time.Now())
	return cb
}

// Name returns the name of the CircuitBreaker
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	cb.currentState(now)
	return cb.state
}

func (cb *CircuitBreaker) currentState(now time.Time) (s State) {
	switch cb.state {
	case StateClosed:
		if cb.interval > 0 && !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state
}

func (cb *CircuitBreaker) setState(newState State, now time.Time) {
	if cb.state == newState {
		return
	}

	prev := cb.state
	cb.state = newState

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, newState)
	}
}

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.clear()

	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.interval > 0 {
			cb.expiry = now.Add(cb.interval)
		} else {
			cb.expiry = zero
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	case StateHalfOpen:
		cb.expiry = zero
	default:
		cb.expiry = zero
	}
}

// Allow checks if a new request is allowed
func (cb *CircuitBreaker) Allow() bool {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state := cb.currentState(now)

	if state == StateOpen {
		return false
	}

	if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		return false
	}

	return true
}

// Execute runs the given function if the CircuitBreaker allows it
// If the function returns an error, it is counted as a failure
func (cb *CircuitBreaker) Execute(req func() (any, error)) (any, error) {
	if !cb.Allow() {
		return nil, ErrOpenState
	}

	cb.mutex.Lock()
	cb.counts.onRequest()
	cb.mutex.Unlock()

	result, err := req()

	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if err != nil {
		cb.counts.onFailure()
		switch cb.state {
		case StateClosed:
			if cb.readyToTrip(cb.counts) {
				cb.setState(StateOpen, time.Now())
			}
		case StateHalfOpen:
			cb.setState(StateOpen, time.Now())
		}
	} else {
		cb.counts.onSuccess()
		if cb.state == StateHalfOpen {
			cb.setState(StateClosed, time.Now())
		}
	}

	return result, err
}

// ErrOpenState is returned when the CircuitBreaker is open
var ErrOpenState = &Error{Msg: "circuit breaker is open"}

type Error struct {
	Msg string
}

func (e *Error) Error() string {
	return e.Msg
}
