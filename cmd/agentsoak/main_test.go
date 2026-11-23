package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// TestByOpStatsIncludesErrorRate pins the per-operation gate signal consumed
// by scripts/check_soak_report.py.
func TestByOpStatsIncludesErrorRate(t *testing.T) {
	gm := newGlobalMetrics()
	gm.record("dense", 0.01, true)
	gm.record("dense", 0.02, true)
	gm.record("dense", 0.03, false)

	stats, ok := gm.byOpStats(time.Now())["dense"].(map[string]any)
	if !ok {
		t.Fatal("missing dense operation stats")
	}
	rate, ok := stats["error_rate"].(float64)
	if !ok {
		t.Fatal("by_op_stats must include error_rate")
	}
	if diff := rate - 1.0/3.0; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("unexpected per-operation error rate: %v", rate)
	}
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

// TestShouldRecordOperationIgnoresShutdownCancellation pins the bounded-soak
// rule that deadline cancellations are not simulator errors.
func TestShouldRecordOperationIgnoresShutdownCancellation(t *testing.T) {
	active := context.Background()
	done, cancel := context.WithCancel(context.Background())
	cancel()

	if !shouldRecordOperation(active, nil) || !shouldRecordOperation(done, nil) {
		t.Fatal("successful operations must always be recorded")
	}
	if !shouldRecordOperation(active, context.Canceled) {
		t.Fatal("cancellation on an active soak must be recorded as an error")
	}
	if shouldRecordOperation(done, context.Canceled) ||
		shouldRecordOperation(done, context.DeadlineExceeded) ||
		shouldRecordOperation(done, fmt.Errorf("rpc failed: %w", context.Canceled)) ||
		shouldRecordOperation(done, status.Error(codes.Canceled, "stream terminated by RST_STREAM")) ||
		shouldRecordOperation(done, status.Error(codes.DeadlineExceeded, "context deadline exceeded")) {
		t.Fatal("shutdown cancellations must not be recorded as soak errors")
	}
	if !shouldRecordOperation(active, status.Error(codes.DeadlineExceeded, "server timeout")) {
		t.Fatal("gRPC deadline errors during an active soak must be recorded")
	}
	if !shouldRecordOperation(done, errors.New("server failure")) {
		t.Fatal("non-cancellation errors after shutdown must still be recorded")
	}
}

// TestRetryIDNotFoundWaitsForAsyncIngest pins the bounded NotFound retry used
// by ID-based soak operations while queued ingestion becomes visible.
func TestRetryIDNotFoundWaitsForAsyncIngest(t *testing.T) {
	calls := 0
	err := retryIDNotFound(context.Background(), func() error {
		calls++
		if calls < 3 {
			return status.Error(codes.NotFound, "id not indexed yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transient NotFound should be retried: %v", err)
	}
	if calls != 3 {
		t.Fatalf("unexpected retry count: %d", calls)
	}
}

// TestRetryIDNotFoundDoesNotMaskOtherErrors verifies that the retry helper is
// narrow enough to keep genuine soak failures visible to the strict gate.
func TestRetryIDNotFoundDoesNotMaskOtherErrors(t *testing.T) {
	calls := 0
	want := errors.New("server failure")
	err := retryIDNotFound(context.Background(), func() error {
		calls++
		return want
	})
	if !errors.Is(err, want) || calls != 1 {
		t.Fatalf("non-NotFound error must not be retried: err=%v calls=%d", err, calls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := retryIDNotFound(ctx, func() error {
		t.Fatal("cancelled retry must not invoke the operation")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled retry must return cancellation: %v", err)
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
