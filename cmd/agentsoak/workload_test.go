package main

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

// TestWeightedSelectorUsesConfiguredMix verifies that the selector honours
// relative weights and is deterministic for a fixed seed.
func TestWeightedSelectorUsesConfiguredMix(t *testing.T) {
	selector, err := newWeightedSelector(map[string]float64{"dense": 3, "hybrid": 1})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	rng := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic test
	for i := 0; i < 4000; i++ {
		counts[selector.pick(rng)]++
	}
	if counts["dense"] <= counts["hybrid"]*2 {
		t.Fatalf("weights not honoured: %v", counts)
	}
	if counts["dense"]+counts["hybrid"] != 4000 {
		t.Fatalf("unexpected total: %v", counts)
	}

	first := rand.New(rand.NewSource(11))  // #nosec G404
	second := rand.New(rand.NewSource(11)) // #nosec G404
	for i := 0; i < 100; i++ {
		if got, want := selector.pick(first), selector.pick(second); got != want {
			t.Fatalf("selector is not deterministic at draw %d: %s != %s", i, got, want)
		}
	}
}

// TestParseWorkloadOverride covers the CLI override used by bounded CI variants.
func TestParseWorkloadOverride(t *testing.T) {
	got, err := parseWorkloadOverride(" dense=25, federated = 2.5 ")
	if err != nil {
		t.Fatal(err)
	}
	if got["dense"] != 25 || got["federated"] != 2.5 {
		t.Fatalf("unexpected override: %v", got)
	}
	if _, err := parseWorkloadOverride("dense"); err == nil {
		t.Fatal("expected malformed override to fail")
	}
	if _, err := parseWorkloadOverride("unknown=1"); err == nil {
		t.Fatal("expected unknown op validation at selector construction")
	}
	if _, err := newWeightedSelector(map[string]float64{"unknown": 1}); err == nil {
		t.Fatal("expected unknown op to be rejected")
	}
}

// TestGlobalMetricsPerOpAndStallReporting verifies the report fields consumed
// by the soak gate checker.
func TestGlobalMetricsPerOpAndStallReporting(t *testing.T) {
	gm := newGlobalMetrics()
	gm.record("dense", 0.01, true)
	gm.record("dense", 0.03, true)
	gm.record("dense", 0.05, false)
	stats := gm.byOpStats(time.Now())
	dense, ok := stats["dense"].(map[string]any)
	if !ok {
		t.Fatalf("missing dense stats: %v", stats)
	}
	if dense["ops"] != int64(3) || dense["errors"] != int64(1) {
		t.Fatalf("unexpected op counters: %v", dense)
	}
	if p99, _ := dense["p99_ms"].(float64); p99 <= 0 {
		t.Fatalf("unexpected p99: %v", p99)
	}

	// A two-minute QPS collapse after a healthy baseline is a stall.
	gm.Start = time.Now().Add(-9 * time.Minute)
	gm.ByOp["federated"] = &opStats{
		Ops:     140,
		Errors:  0,
		Windows: map[int]int64{5: 60, 6: 60, 7: 10, 8: 10},
		Last:    time.Now(),
	}
	stalls := gm.stalls(time.Now())
	if len(stalls) != 1 || stalls[0]["op"] != "federated" {
		t.Fatalf("unexpected stalls: %v", stalls)
	}
	if drop, _ := stalls[0]["drop_pct"].(float64); drop < 80 {
		t.Fatalf("unexpected drop: %v", stalls[0])
	}
}

// TestWeightedSelectorRejectsInvalidWorkloads covers the validation that keeps
// a bad workload from silently producing an empty or unknown operation mix.
func TestWeightedSelectorRejectsInvalidWorkloads(t *testing.T) {
	cases := []struct {
		name    string
		weights map[string]float64
		wantErr string
	}{
		{"empty", map[string]float64{}, "empty workload"},
		{"negative weight", map[string]float64{"dense": -1}, "negative weight"},
		{"all zero weights", map[string]float64{"dense": 0, "hybrid": 0}, "positive value"},
		{"unknown op", map[string]float64{"unknown": 1}, "unknown workload op"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newWeightedSelector(tc.weights)
			if err == nil {
				t.Fatalf("expected error containing %q", tc.wantErr)
			}
		})
	}

	selector, err := newWeightedSelector(map[string]float64{"dense": 1, "hybrid": 0})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(3)) // #nosec G404 -- deterministic test
	for i := 0; i < 50; i++ {
		if op := selector.pick(rng); op != "dense" {
			t.Fatalf("zero-weight op was selected: %q", op)
		}
	}
}

// TestWeightedSelectorDeterministicAcrossMapOrder pins the sorted cumulative
// distribution: identical weights must give identical draws regardless of how
// the map was built or iterated.
func TestWeightedSelectorDeterministicAcrossMapOrder(t *testing.T) {
	first := map[string]float64{"dense": 3, "hybrid": 1, "sparse": 2}
	second := map[string]float64{"sparse": 2, "hybrid": 1, "dense": 3}
	a, err := newWeightedSelector(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newWeightedSelector(second)
	if err != nil {
		t.Fatal(err)
	}
	firstRNG := rand.New(rand.NewSource(19))  // #nosec G404 -- deterministic test
	secondRNG := rand.New(rand.NewSource(19)) // #nosec G404 -- deterministic test
	for i := 0; i < 200; i++ {
		if got, want := b.pick(secondRNG), a.pick(firstRNG); got != want {
			t.Fatalf("draw %d differs across map order: %q != %q", i, got, want)
		}
	}
}

// TestParseWorkloadOverrideEdgeCases covers empty entries, duplicates, and the
// invalid syntax that must fail before a long soak is started.
func TestParseWorkloadOverrideEdgeCases(t *testing.T) {
	got, err := parseWorkloadOverride(" dense=25,, hybrid = 5, ")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["dense"] != 25 || got["hybrid"] != 5 {
		t.Fatalf("unexpected override: %v", got)
	}
	got, err = parseWorkloadOverride("dense=25,dense=5")
	if err != nil || got["dense"] != 5 {
		t.Fatalf("duplicate override must use last value: %v, %v", got, err)
	}

	for _, input := range []string{"", "   ", "dense", "dense=", "dense=abc", "dense=-1", "dense=NaN", "dense=+Inf", "unknown=1", "=1"} {
		if _, err := parseWorkloadOverride(input); err == nil {
			t.Fatalf("expected error for override %q", input)
		}
	}
	for _, weight := range []float64{0, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := newWeightedSelector(map[string]float64{"dense": weight}); err == nil {
			t.Fatalf("non-finite or zero selector weight must be rejected: %v", weight)
		}
	}
}

// TestTemporalSearchTypeMapping pins the operation-to-action mapping used by
// the temporal slice of the workload.
func TestTemporalSearchTypeMapping(t *testing.T) {
	cases := map[string]string{
		"temporal_asof":   "as_of",
		"temporal_range":  "range",
		"temporal_window": "sliding_window",
		"unknown":         "unknown",
	}
	for op, want := range cases {
		if got := temporalSearchType(op); got != want {
			t.Fatalf("temporalSearchType(%q) = %q, want %q", op, got, want)
		}
	}
}

// TestGlobalMetricsStallDetectionBoundaries verifies that only sustained
// collapses on busy operations become report stalls, in stable op order.
func TestGlobalMetricsStallDetectionBoundaries(t *testing.T) {
	gm := newGlobalMetrics()
	now := time.Now()
	gm.Start = now.Add(-10 * time.Minute)
	if stalls := gm.stalls(now); len(stalls) != 0 {
		t.Fatalf("empty metrics reported stalls: %v", stalls)
	}
	gm.ByOp["stable"] = &opStats{Windows: map[int]int64{0: 10, 1: 10, 2: 10, 3: 10}, Last: now}
	gm.ByOp["short"] = &opStats{Windows: map[int]int64{0: 100, 1: 100}, Last: now}
	gm.ByOp["sparse"] = &opStats{Windows: map[int]int64{0: 200, 1: 200, 2: 1, 3: 1}, Last: now}
	gm.ByOp["dense"] = &opStats{Windows: map[int]int64{0: 20, 1: 20, 2: 1, 3: 1}, Last: now}

	stalls := gm.stalls(now)
	if len(stalls) != 2 || stalls[0]["op"] != "dense" || stalls[1]["op"] != "sparse" {
		t.Fatalf("stalls = %v, want sorted dense and sparse", stalls)
	}
	for _, stall := range stalls {
		if stall["duration_s"] != 120 {
			t.Fatalf("unexpected stall window: %v", stall)
		}
		baseline, _ := stall["baseline_qps"].(float64)
		current, _ := stall["current_qps"].(float64)
		if baseline <= 0 || current <= 0 || current >= baseline {
			t.Fatalf("unexpected stall rates: %v", stall)
		}
		if drop, _ := stall["drop_pct"].(float64); drop < 95 {
			t.Fatalf("unexpected drop: %v", stall)
		}
	}
}

// TestPercentileMSBoundaries and the future-timestamp check keep report
// percentiles finite even when a stalled or clock-skewed run reports no data.
func TestPercentileMSBoundaries(t *testing.T) {
	if got := percentileMS(nil, 50); got != 0 {
		t.Fatalf("empty percentile = %v, want 0", got)
	}
	lats := []float64{0.1, 0.2, 0.3}
	if got := percentileMS(lats, 0); got != 100 {
		t.Fatalf("p0 = %v, want 100", got)
	}
	if got := percentileMS(lats, 50); got != 200 {
		t.Fatalf("p50 = %v, want 200", got)
	}
	if got := percentileMS(lats, 100); got != 300 {
		t.Fatalf("p100 = %v, want 300", got)
	}
	if got := percentileMS(lats, 200); got != 300 {
		t.Fatalf("clamped percentile = %v, want 300", got)
	}
}

func TestByOpStatsWithFutureSampleTime(t *testing.T) {
	gm := newGlobalMetrics()
	gm.record("dense", 0.01, true)
	stats := gm.byOpStats(time.Now().Add(-time.Hour))
	dense, ok := stats["dense"].(map[string]any)
	if !ok {
		t.Fatalf("missing dense stats: %v", stats)
	}
	if qps, _ := dense["qps"].(float64); qps <= 0 {
		t.Fatalf("qps must stay positive when elapsed is non-positive: %v", dense)
	}
}

// FuzzParseWorkloadOverride feeds arbitrary CLI override strings through the
// same parser used by -workload and the selector that consumes its result.
func FuzzParseWorkloadOverride(f *testing.F) {
	f.Add("dense=25,hybrid=10")
	f.Add(" dense = 2.5 , sparse=0 ")
	f.Add("")
	f.Add("dense")
	f.Add("dense=-1")
	f.Add("dense=NaN")
	f.Add("dense=+Inf")
	f.Add("unknown=1")
	f.Add("dense=25,dense=5")
	f.Fuzz(func(t *testing.T, override string) {
		if len(override) > 512 {
			return
		}
		weights, err := parseWorkloadOverride(override)
		if err != nil {
			return
		}
		if len(weights) == 0 {
			t.Fatal("successful parse returned no operations")
		}
		for op, weight := range weights {
			if !knownWorkloadOp(op) || !finiteWeight(weight) || weight < 0 {
				t.Fatalf("invalid parsed override: %q=%v", op, weight)
			}
		}
		selector, err := newWeightedSelector(weights)
		if err != nil {
			t.Fatalf("parsed override rejected by selector: %v", err)
		}
		rng := rand.New(rand.NewSource(1)) // #nosec G404 -- deterministic test
		for i := 0; i < 20; i++ {
			if op := selector.pick(rng); !knownWorkloadOp(op) {
				t.Fatalf("selector produced unknown op %q", op)
			}
		}
	})
}
