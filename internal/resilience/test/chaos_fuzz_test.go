package test

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

// FuzzChaosCollectResultsStats verifies the aggregate statistics computed by
// collectResults for arbitrary result sets: counts, success/error rates,
// min/max/average latency and start/end timestamps must all be exact, and the
// computation must be deterministic across runs.
func FuzzChaosCollectResultsStats(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{
		0x10, 0x27, 0, 0, 0, 0, 0, 0, // latency 10000ns
		0xe8, 0x03, 0, 0, 0, 0, 0, 0, // timestamp 1000ns
		1, // success
	})
	f.Add([]byte{
		0x00, 0x10, 0, 0, 0, 0, 0, 0, // latency 4096ns
		0x00, 0x0f, 0, 0, 0, 0, 0, 0, // timestamp 3840ns
		0,                                              // failure
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, // latency near int64 max → reduced by modulo
		0x00, 0x00, 0, 0, 0, 0, 0, 0,
		1,
	})
	// Mixed multi-record payload.
	mixed := make([]byte, 0, 17*3)
	for i := byte(0); i < 3; i++ {
		var rec [17]byte
		binary.LittleEndian.PutUint64(rec[0:8], uint64(i+1)*1000)
		binary.LittleEndian.PutUint64(rec[8:16], uint64(i+1)*500)
		rec[16] = i & 1
		mixed = append(mixed, rec[:]...)
	}
	f.Add(mixed)
	f.Add(make([]byte, 17*16))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			return
		}

		const recordSize = 17
		// Bound latency/timestamps to < 1h so collectResults' min-tracking
		// (seeded with time.Hour) always observes the real minimum.
		const latencyBound = time.Duration(1_000_000_000_000)

		n := len(data) / recordSize
		results := make([]*TestResult, 0, n)
		var sumLatency time.Duration
		minLatency := time.Hour
		var maxLatency time.Duration
		minTS := int64(math.MaxInt64)
		maxTS := int64(math.MinInt64)
		successCount := 0

		for i := 0; i < n; i++ {
			off := i * recordSize
			lat := time.Duration(binary.LittleEndian.Uint64(data[off:off+8]) % uint64(latencyBound))
			tsNS := int64(binary.LittleEndian.Uint64(data[off+8:off+16]) % uint64(latencyBound))
			ok := data[off+16]&1 == 1

			if ok {
				successCount++
			}
			sumLatency += lat
			if lat < minLatency {
				minLatency = lat
			}
			if lat > maxLatency {
				maxLatency = lat
			}
			if tsNS < minTS {
				minTS = tsNS
			}
			if tsNS > maxTS {
				maxTS = tsNS
			}

			results = append(results, &TestResult{
				WorkerID:  i % 8,
				RequestID: i,
				Latency:   lat,
				Success:   ok,
				Timestamp: time.Unix(0, tsNS),
			})
		}

		run := func() *TestResults {
			ch := make(chan *TestResult, len(results))
			for _, r := range results {
				ch <- r
			}
			close(ch)
			rts := &ResilienceTestSuite{}
			return rts.collectResults(ch)
		}

		got := run()

		if got.TotalRequests != n {
			t.Fatalf("TotalRequests = %d, want %d", got.TotalRequests, n)
		}
		if len(got.Results) != n {
			t.Fatalf("len(Results) = %d, want %d", len(got.Results), n)
		}
		if got.SuccessfulRequests != successCount {
			t.Fatalf("SuccessfulRequests = %d, want %d", got.SuccessfulRequests, successCount)
		}
		if got.FailedRequests != n-successCount {
			t.Fatalf("FailedRequests = %d, want %d", got.FailedRequests, n-successCount)
		}
		if n == 0 {
			return
		}

		if want := float64(successCount) / float64(n); got.SuccessRate != want {
			t.Fatalf("SuccessRate = %v, want %v", got.SuccessRate, want)
		}
		if want := float64(n-successCount) / float64(n); got.ErrorRate != want {
			t.Fatalf("ErrorRate = %v, want %v", got.ErrorRate, want)
		}
		if sum := got.SuccessRate + got.ErrorRate; sum < 0.999999 || sum > 1.000001 {
			t.Fatalf("SuccessRate+ErrorRate = %v, want ~1", sum)
		}
		if got.MinLatency != minLatency {
			t.Fatalf("MinLatency = %v, want %v", got.MinLatency, minLatency)
		}
		if got.MaxLatency != maxLatency {
			t.Fatalf("MaxLatency = %v, want %v", got.MaxLatency, maxLatency)
		}
		if want := sumLatency / time.Duration(n); got.AverageLatency != want {
			t.Fatalf("AverageLatency = %v, want %v", got.AverageLatency, want)
		}
		if got.AverageLatency < got.MinLatency || got.AverageLatency > got.MaxLatency {
			t.Fatalf("AverageLatency %v outside [%v, %v]",
				got.AverageLatency, got.MinLatency, got.MaxLatency)
		}
		if got.StartTime.UnixNano() != minTS {
			t.Fatalf("StartTime = %d, want earliest timestamp %d", got.StartTime.UnixNano(), minTS)
		}
		if got.EndTime.UnixNano() != maxTS {
			t.Fatalf("EndTime = %d, want latest timestamp %d", got.EndTime.UnixNano(), maxTS)
		}

		// Determinism: a second run over the same results must agree exactly.
		again := run()
		if again.TotalRequests != got.TotalRequests ||
			again.SuccessfulRequests != got.SuccessfulRequests ||
			again.FailedRequests != got.FailedRequests ||
			again.SuccessRate != got.SuccessRate ||
			again.ErrorRate != got.ErrorRate ||
			again.MinLatency != got.MinLatency ||
			again.MaxLatency != got.MaxLatency ||
			again.AverageLatency != got.AverageLatency ||
			again.StartTime.UnixNano() != got.StartTime.UnixNano() ||
			again.EndTime.UnixNano() != got.EndTime.UnixNano() {
			t.Fatalf("collectResults not deterministic: first=%+v second=%+v", got, again)
		}
	})
}

// FuzzChaosInjectorDeterminism verifies the deterministic operating regimes of
// ChaosInjector.ShouldInject: a stopped injector never injects, probability 0
// never injects, probability >= 1 always injects for a matching target, and
// the injected-fault counter tracks exactly the injections that occurred.
func FuzzChaosInjectorDeterminism(f *testing.F) {
	f.Add("svc-a", "svc-a", 0.0, false)
	f.Add("svc-a", "svc-a", 1.0, true)
	f.Add("svc-a", "all", 0.5, true)
	f.Add("svc-a", "svc-b", 1.0, true)
	f.Add("svc-a", "svc-a", -0.5, true)
	f.Add("svc-b", "all", 2.0, false)

	f.Fuzz(func(t *testing.T, target, cfgTarget string, prob float64, running bool) {
		if len(target) > 256 || len(cfgTarget) > 256 {
			return
		}
		if math.IsNaN(prob) || math.IsInf(prob, 0) || prob > 1e9 || prob < -1e9 {
			return
		}

		inj := NewChaosInjector()
		inj.AddConfig(ChaosConfig{
			Type:            ChaosError,
			Probability:     prob,
			TargetComponent: cfgTarget,
		})

		match := cfgTarget == target || cfgTarget == "all"

		// Stopped injector: never injects, regardless of configuration.
		got, cfg := inj.ShouldInject(target)
		if got {
			t.Fatalf("stopped injector injected for target %q", target)
		}
		if cfg != (ChaosConfig{}) {
			t.Fatalf("stopped injector returned config %+v, want zero value", cfg)
		}

		inj.Start()
		got, cfg = inj.ShouldInject(target)

		switch {
		case match && prob >= 1:
			// rand.Float64() is always in [0,1), so this must inject.
			if !got {
				t.Fatalf("probability %v with matching target must inject", prob)
			}
		case !match || prob <= 0:
			// No match, or probability below rand.Float64()'s range: never injects.
			if got {
				t.Fatalf("unexpected injection: match=%v probability=%v", match, prob)
			}
		default:
			// Probabilistic range: the returned config must be the one registered.
			if got && (cfg.Type != ChaosError || cfg.Probability != prob ||
				cfg.TargetComponent != cfgTarget) {
				t.Fatalf("injected with wrong config: %+v", cfg)
			}
		}

		var wantCount int64
		if got {
			wantCount = 1
		}
		if count := inj.GetInjectedCount(); count != wantCount {
			t.Fatalf("injected count = %d, want %d", count, wantCount)
		}
	})
}
