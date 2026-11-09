package breaker

import (
	"errors"
	"testing"
	"time"
)

// FuzzCountsSequence drives the raw Counts state machine with a fuzzed
// sequence of request+result rounds and checks its bookkeeping invariants.
func FuzzCountsSequence(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 1, 1})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{1, 0, 1, 0, 1})
	f.Add([]byte{0, 1, 1, 1, 0, 0, 1})
	f.Add([]byte{1, 1, 1, 1, 1, 1, 1, 1})

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 256 {
			return
		}

		var a, b Counts
		for _, op := range ops {
			// Mirror one Execute() round: request then result.
			a.onRequest()
			b.onRequest()
			if op%2 == 0 {
				a.onSuccess()
				b.onSuccess()
			} else {
				a.onFailure()
				b.onFailure()
			}
		}

		// Determinism: identical sequences yield identical counts.
		if a != b {
			t.Fatalf("non-deterministic counts: %+v vs %+v", a, b)
		}

		// Invariants of the counters.
		if a.Requests != uint32(len(ops)) {
			t.Fatalf("Requests = %d, want %d", a.Requests, len(ops))
		}
		if a.TotalSuccesses+a.TotalFailures != a.Requests {
			t.Fatalf("successes %d + failures %d != requests %d",
				a.TotalSuccesses, a.TotalFailures, a.Requests)
		}
		if a.ConsecutiveSuccesses != 0 && a.ConsecutiveFailures != 0 {
			t.Fatalf("consecutive counters not mutually exclusive: %+v", a)
		}
		if a.ConsecutiveSuccesses > a.TotalSuccesses {
			t.Fatalf("consecutive successes exceed total: %+v", a)
		}
		if a.ConsecutiveFailures > a.TotalFailures {
			t.Fatalf("consecutive failures exceed total: %+v", a)
		}
		if len(ops) > 0 {
			if ops[len(ops)-1]%2 == 0 && a.ConsecutiveSuccesses == 0 {
				t.Fatalf("last op succeeded but no consecutive successes: %+v", a)
			}
			if ops[len(ops)-1]%2 == 1 && a.ConsecutiveFailures == 0 {
				t.Fatalf("last op failed but no consecutive failures: %+v", a)
			}
		}
	})
}

// refBreaker is a simple sequential model of the CircuitBreaker state
// machine (time-based transitions are excluded by using a large Timeout).
type refBreaker struct {
	state   State
	maxReqs uint32
	tripAt  uint32
	counts  Counts
}

func (r *refBreaker) allow() bool {
	if r.state == StateOpen {
		return false
	}
	if r.state == StateHalfOpen && r.counts.Requests >= r.maxReqs {
		return false
	}
	return true
}

// FuzzCircuitBreakerExecute replays a fuzzed sequence of successful and
// failing executions against a real CircuitBreaker and a reference model,
// asserting that Allow/Execute/State agree with the model after every step.
func FuzzCircuitBreakerExecute(f *testing.F) {
	f.Add([]byte{0, 1, 1, 1, 1}, uint32(2), uint8(1), "cb")
	f.Add([]byte{}, uint32(0), uint8(5), "empty")
	f.Add([]byte{0, 0, 0, 0}, uint32(4), uint8(3), "all-ok")
	f.Add([]byte{1, 1}, uint32(1), uint8(1), "open-fast")
	f.Add([]byte{1, 0, 1, 0, 1}, uint32(3), uint8(2), "mix")
	f.Add([]byte{0, 1, 1, 1, 0, 1, 1, 1}, uint32(0), uint8(0), "zero-max")

	f.Fuzz(func(t *testing.T, ops []byte, maxRequests uint32, tripThreshold uint8, name string) {
		if len(ops) > 32 {
			return
		}
		if len(name) > 256 {
			return
		}

		tripAt := uint32(tripThreshold)
		cb := NewCircuitBreaker(Settings{
			Name:        name,
			MaxRequests: maxRequests,
			// No time-based transitions can occur during the test run.
			Timeout: time.Hour,
			ReadyToTrip: func(c Counts) bool {
				return c.ConsecutiveFailures >= tripAt
			},
		})
		if cb.Name() != name {
			t.Fatalf("Name() = %q, want %q", cb.Name(), name)
		}

		model := &refBreaker{state: StateClosed, maxReqs: maxRequests, tripAt: tripAt}
		if model.maxReqs == 0 {
			model.maxReqs = 1
		}

		fuzzErr := errors.New("fuzz-failure")
		for i, op := range ops {
			wantAllowed := model.allow()
			if got := cb.Allow(); got != wantAllowed {
				t.Fatalf("op %d: Allow() = %v, want %v (state %v)", i, got, wantAllowed, model.state)
			}

			failed := op%2 == 1
			gotVal, gotErr := cb.Execute(func() (any, error) {
				if failed {
					return nil, fuzzErr
				}
				return "ok", nil
			})

			if !wantAllowed {
				if gotErr != ErrOpenState {
					t.Fatalf("op %d: blocked Execute err = %v, want ErrOpenState", i, gotErr)
				}
				if gotVal != nil {
					t.Fatalf("op %d: blocked Execute returned %v, want nil", i, gotVal)
				}
				continue
			}

			if failed {
				if !errors.Is(gotErr, fuzzErr) {
					t.Fatalf("op %d: failed Execute err = %v, want fuzzErr", i, gotErr)
				}
			} else {
				if gotErr != nil {
					t.Fatalf("op %d: successful Execute err = %v", i, gotErr)
				}
				if gotVal != "ok" {
					t.Fatalf("op %d: successful Execute returned %v", i, gotVal)
				}
			}

			// Advance the model exactly as Execute does.
			model.counts.Requests++
			if failed {
				model.counts.TotalFailures++
				model.counts.ConsecutiveFailures++
				model.counts.ConsecutiveSuccesses = 0
				switch model.state {
				case StateClosed:
					if model.counts.ConsecutiveFailures >= model.tripAt {
						model.state = StateOpen
						model.counts = Counts{}
					}
				case StateHalfOpen:
					model.state = StateOpen
					model.counts = Counts{}
				}
			} else {
				model.counts.TotalSuccesses++
				model.counts.ConsecutiveSuccesses++
				model.counts.ConsecutiveFailures = 0
				if model.state == StateHalfOpen {
					model.state = StateClosed
					model.counts = Counts{}
				}
			}

			if got := cb.State(); got != model.state {
				t.Fatalf("op %d: State() = %v, want %v", i, got, model.state)
			}
		}

		if got := cb.State(); got != model.state {
			t.Fatalf("final State() = %v, want %v", got, model.state)
		}
		if got, want := cb.Allow(), model.allow(); got != want {
			t.Fatalf("final Allow() = %v, want %v", got, want)
		}
	})
}
