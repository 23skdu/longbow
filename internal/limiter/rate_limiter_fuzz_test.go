package limiter

import (
	"testing"

	"golang.org/x/time/rate"
)

// FuzzRateLimiterConfig checks the Config → token-bucket translation and the
// basic token-bucket bounds (a full bucket admits exactly `burst` immediate
// requests and tokens never leave [0, burst]).
func FuzzRateLimiterConfig(f *testing.F) {
	f.Add(10, 20)    // plain enabled limiter
	f.Add(0, 0)      // disabled via RPS=0
	f.Add(-5, 10)    // disabled via negative RPS
	f.Add(5, -1)     // burst <= 0 defaults to RPS
	f.Add(1, 1)      // minimal enabled limiter
	f.Add(100, 4096) // large burst, still walkable
	f.Add(3, 0)      // burst defaults to RPS
	f.Add(4096, 1)   // burst defaults to a large RPS

	f.Fuzz(func(t *testing.T, rps, burst int) {
		l := NewRateLimiter(Config{RPS: rps, Burst: burst})

		if rps <= 0 {
			if l.enabled {
				t.Errorf("RPS=%d: limiter must be disabled", rps)
			}
			if l.limiter != nil {
				t.Errorf("RPS=%d: disabled limiter must not wrap a token bucket", rps)
			}
			return
		}

		// Bound the walk below: the bucket must be small enough that
		// draining it one request at a time stays cheap.
		if rps > 4096 || burst > 4096 {
			return
		}

		if !l.enabled {
			t.Fatalf("RPS=%d: limiter must be enabled", rps)
		}
		if l.limiter == nil {
			t.Fatal("enabled limiter has no token bucket")
		}

		wantBurst := burst
		if wantBurst <= 0 {
			wantBurst = rps // NewRateLimiter defaults burst to RPS
		}

		if got := l.limiter.Limit(); got != rate.Limit(rps) {
			t.Errorf("limit = %v, want %d", got, rps)
		}
		if got := l.limiter.Burst(); got != wantBurst {
			t.Errorf("burst = %d, want %d", got, wantBurst)
		}
		if got := l.limiter.Tokens(); got != float64(wantBurst) {
			t.Errorf("initial tokens = %v, want %d", got, wantBurst)
		}

		// A full bucket must admit exactly wantBurst immediate requests;
		// refills during the walk can only help, never hurt.
		for i := 0; i < wantBurst; i++ {
			if !l.limiter.Allow() {
				t.Fatalf("Allow() = false on request %d of a %d-token bucket", i+1, wantBurst)
			}
		}

		// Tokens can never leave [0, burst].
		if tok := l.limiter.Tokens(); tok < 0 || tok > float64(wantBurst) {
			t.Errorf("tokens after draining = %v, want within [0, %d]", tok, wantBurst)
		}
	})
}
