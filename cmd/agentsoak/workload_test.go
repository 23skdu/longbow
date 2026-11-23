package main

import (
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
