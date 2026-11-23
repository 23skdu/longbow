package main

import (
	"math/rand"
	"testing"
)

// TestBuildSchema runs the test build schema operation.
func TestBuildSchema(t *testing.T) {
	s := buildSchema(4)
	if s == nil || s.NumFields() != 5 {
		t.Fatalf("unexpected schema")
	}
}

// TestRandVec runs the test rand vec operation.
func TestRandVec(t *testing.T) {
	v := randVec(rand.New(rand.NewSource(1)), 4)
	if len(v) != 4 {
		t.Fatal("bad vec")
	}
	_ = v
}

// TestCrossVerificationCoversFederatedOnly pins the soak gate's verification
// signal for the federated-only CI variant, which has no dense reads to carve
// into cross_read.
func TestCrossVerificationCoversFederatedOnly(t *testing.T) {
	cfg := defaultConfig()
	cfg.Agents = 2
	cfg.AgentsPerNamespace = 0
	topo := newTopologyUnchecked(cfg)
	a := &agent{id: 0, topo: topo}

	if !a.isCrossVerification("cross_read") {
		t.Fatal("cross_read must always count as verification")
	}
	if !a.isCrossVerification("federated") {
		t.Fatal("successful federated fan-out over another agent's namespace must count as verification")
	}
	if a.isCrossVerification("dense") {
		t.Fatal("local dense reads must not count as cross-agent verification")
	}
}

// FuzzRandVec runs the fuzz rand vec operation.
func FuzzRandVec(f *testing.F) {
	f.Add(4)
	f.Fuzz(func(t *testing.T, d int) {
		if d < 1 {
			d = 1
		}
		if d > 64 {
			d = 64
		}
		v := randVec(rand.New(rand.NewSource(1)), d)
		if len(v) != d {
			t.Fatal("bad vec")
		}
	})
}
