package resilience

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// retryablePatterns is the documented error-substring contract of
// DefaultRetryableFunc; it acts as the independent oracle for the fuzz target.
var retryablePatterns = []string{
	"connection refused",
	"connection reset",
	"timeout",
	"deadline exceeded",
	"temporary failure",
	"service unavailable",
	"resource temporarily unavailable",
	"network is unreachable",
	"no such host",
	"connection timed out",
	"read: connection reset",
	"write: connection reset",
}

// FuzzDefaultRetryableFuncMatchesSpec verifies that DefaultRetryableFunc is
// exactly "any documented retryable substring appears in the error message"
// for arbitrary messages, and that a nil error is never retryable.
func FuzzDefaultRetryableFuncMatchesSpec(f *testing.F) {
	f.Add("")
	f.Add("connection refused")
	f.Add("dial tcp: i/o timeout")
	f.Add("random failure not in the retryable list")
	f.Add("no such host")
	f.Add("read: connection reset by peer")
	f.Add("deadline exceeded while waiting")

	f.Fuzz(func(t *testing.T, msg string) {
		if len(msg) > 4096 {
			return
		}

		want := false
		for _, pattern := range retryablePatterns {
			if strings.Contains(msg, pattern) {
				want = true
				break
			}
		}

		if got := DefaultRetryableFunc(errors.New(msg)); got != want {
			t.Fatalf("DefaultRetryableFunc(%q) = %v, want %v", msg, got, want)
		}
		if DefaultRetryableFunc(nil) {
			t.Fatalf("DefaultRetryableFunc(nil) must be false")
		}
	})
}

// FuzzCalculateDelayBounds verifies the retry backoff math: for a well-formed
// policy the computed delay is always deterministic, non-negative and never
// exceeds MaxDelay, regardless of the attempt number.
func FuzzCalculateDelayBounds(f *testing.F) {
	f.Add(int64(100_000_000), int64(30_000_000_000), 2.0, 1)
	f.Add(int64(0), int64(0), 1.0, 0)
	f.Add(int64(1_000_000_000), int64(10_000_000_000), 1.5, 10)
	f.Add(int64(5_000_000), int64(1_000_000_000), 4.0, 64)
	f.Add(int64(1), int64(9_000_000_000_000_000), 1.0, 1000000)
	f.Add(int64(-1), int64(1000), 2.0, 3)
	f.Add(int64(1000), int64(500), 2.0, 3)

	f.Fuzz(func(t *testing.T, initial, maxDelay int64, multiplier float64, attempt int) {
		// Bound inputs: only well-formed retry policies are in scope.
		// Values above 2^53 keep float64 conversion of durations exact.
		const maxDuration = int64(9_000_000_000_000_000)
		if initial < 0 || initial > maxDuration {
			return
		}
		if maxDelay < 0 || maxDelay > maxDuration {
			return
		}
		if initial > maxDelay {
			return
		}
		if math.IsNaN(multiplier) || multiplier < 1 || multiplier > 4 {
			return
		}
		if attempt < 0 || attempt > 64 {
			return
		}

		policy := &RetryPolicy{
			InitialDelay: time.Duration(initial),
			MaxDelay:     time.Duration(maxDelay),
			Multiplier:   multiplier,
		}

		delay := calculateDelay(policy, attempt)
		if delay < 0 {
			t.Fatalf("calculateDelay(%v, %v, %v, %d) = negative %v",
				policy.InitialDelay, policy.MaxDelay, multiplier, attempt, delay)
		}
		if delay > time.Duration(maxDelay) {
			t.Fatalf("calculateDelay(%v, %v, %v, %d) = %v, exceeds MaxDelay",
				policy.InitialDelay, policy.MaxDelay, multiplier, attempt, delay)
		}
		if again := calculateDelay(policy, attempt); again != delay {
			t.Fatalf("calculateDelay not deterministic: %v then %v", delay, again)
		}
	})
}
