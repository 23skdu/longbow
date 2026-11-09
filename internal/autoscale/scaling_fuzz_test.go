package autoscale

import (
	"testing"
	"time"
)

// hugeWindowInterval makes the rolling window deterministic under fuzzing:
// a single test run never advances far enough for the window to slide, so
// Sum/Average reflect exactly the values that were added.
const hugeWindowInterval = time.Duration(1) << 62

func FuzzRollingWindowSum(f *testing.F) {
	f.Add(uint8(4), []byte{1, 2, 3})
	f.Add(uint8(1), []byte{})
	f.Add(uint8(8), []byte{255, 128, 0, 1, 254})
	f.Add(uint8(2), []byte{10, 200, 15})
	f.Add(uint8(16), []byte{5, 5, 5, 5, 5, 5, 5, 5})
	f.Add(uint8(64), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	f.Fuzz(func(t *testing.T, capacity uint8, deltas []byte) {
		if capacity < 1 || capacity > 64 {
			return
		}
		if len(deltas) > 64 {
			return
		}

		rw := NewRollingWindow(hugeWindowInterval, int(capacity))
		var want int64
		for _, b := range deltas {
			delta := int64(int8(b)) * 1000
			rw.Add(delta)
			want += delta
		}

		if got := rw.Sum(); got != want {
			t.Fatalf("Sum() = %d, want %d", got, want)
		}
		if got, wantAvg := rw.Average(), float64(want)/float64(capacity); got != wantAvg {
			t.Fatalf("Average() = %v, want %v", got, wantAvg)
		}

		// Determinism: an identical window fed identical deltas must agree.
		rw2 := NewRollingWindow(hugeWindowInterval, int(capacity))
		for _, b := range deltas {
			rw2.Add(int64(int8(b)) * 1000)
		}
		if rw2.Sum() != rw.Sum() || rw2.Average() != rw.Average() {
			t.Fatalf("identical inputs produced different results: (%d, %v) vs (%d, %v)",
				rw.Sum(), rw.Average(), rw2.Sum(), rw2.Average())
		}
	})
}

func FuzzLoadSnapshotHealth(f *testing.F) {
	f.Add(int64(0), int64(0))
	f.Add(int64(3000), int64(0))
	f.Add(int64(60000), int64(100))
	f.Add(int64(1000), int64(5000))
	f.Add(int64(100), int64(100))
	f.Add(int64(0), int64(100000))

	f.Fuzz(func(t *testing.T, first int64, second int64) {
		if first < 0 || second < 0 {
			return
		}
		if first > 100_000_000 || second > 100_000_000 {
			return
		}

		cfg := ScalingConfig{
			MinIndexingWorkers:  4,
			MaxIndexingWorkers:  16,
			MinIngestionWorkers: 4,
			MaxIngestionWorkers: 16,
			TargetQPSPerWorker:  50.0,
			ScaleUpThreshold:    0.8,
			ScaleDownThreshold:  0.2,
		}
		newScaler := func() *AutoScaler {
			return &AutoScaler{
				searchWindow: NewRollingWindow(hugeWindowInterval, 60),
				ingestWindow: NewRollingWindow(hugeWindowInterval, 60),
				config:       cfg,
			}
		}
		// Reference policy: the health thresholds encoded in GetLoadSnapshot.
		refHealth := func(qps float64) ClusterHealth {
			switch {
			case qps > (cfg.TargetQPSPerWorker * float64(cfg.MaxIndexingWorkers)):
				return HealthCritical
			case qps > (cfg.TargetQPSPerWorker * 0.8):
				return HealthDegraded
			default:
				return HealthHealthy
			}
		}

		as := newScaler()
		as.searchWindow.Add(first)
		snap1 := as.GetLoadSnapshot()
		as.searchWindow.Add(second)
		snap2 := as.GetLoadSnapshot()

		// Exact QPS values.
		if want := float64(first) / 60.0; snap1.SearchQPS != want {
			t.Fatalf("first SearchQPS = %v, want %v", snap1.SearchQPS, want)
		}
		if want := float64(first+second) / 60.0; snap2.SearchQPS != want {
			t.Fatalf("second SearchQPS = %v, want %v", snap2.SearchQPS, want)
		}

		// Health must match the reference policy classification.
		if snap1.Health != refHealth(snap1.SearchQPS) {
			t.Fatalf("first health = %v, want %v for qps %v", snap1.Health, refHealth(snap1.SearchQPS), snap1.SearchQPS)
		}
		if snap2.Health != refHealth(snap2.SearchQPS) {
			t.Fatalf("second health = %v, want %v for qps %v", snap2.Health, refHealth(snap2.SearchQPS), snap2.SearchQPS)
		}

		// Monotonicity: more load can never report less load or better health.
		if snap2.SearchQPS < snap1.SearchQPS {
			t.Fatalf("SearchQPS decreased: %v -> %v", snap1.SearchQPS, snap2.SearchQPS)
		}
		if snap2.Health < snap1.Health {
			t.Fatalf("health degraded without load increase: %v -> %v", snap1.Health, snap2.Health)
		}

		// Determinism: one combined add must equal the two-step sequence.
		as2 := newScaler()
		as2.searchWindow.Add(first + second)
		snap3 := as2.GetLoadSnapshot()
		if snap3.SearchQPS != snap2.SearchQPS || snap3.Health != snap2.Health {
			t.Fatalf("non-deterministic snapshot: (%v, %v) vs (%v, %v)",
				snap2.SearchQPS, snap2.Health, snap3.SearchQPS, snap3.Health)
		}
	})
}
