package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

// authBearer reads the credential installed on a client without issuing an RPC.
func authBearer(c *client.SmartClient) string {
	md, _ := metadata.FromOutgoingContext(c.WithAuthContext(context.Background()))
	values := md.Get("authorization")
	if len(values) != 1 {
		return ""
	}
	return strings.TrimPrefix(values[0], "Bearer ")
}

// TestSetKeyForSwitchesScopedCredentials pins the credential selection that
// enforces namespace RBAC in both the default and grouped layouts.
func TestSetKeyForSwitchesScopedCredentials(t *testing.T) {
	cfg := defaultConfig()
	topo := newTopologyUnchecked(cfg)
	keys := agentKeys{Writer: "writer-key", Reader: "reader-key", Federated: "federated-key"}
	a := &agent{id: 0, topo: topo, keys: keys}
	c := &client.SmartClient{} // zero value is enough for metadata inspection

	cases := []struct {
		name, ns, op, want string
	}{
		{"federated ignores owned namespace", topo.private[0], "federated", keys.Federated},
		{"federated ignores read-only namespace", topo.shared[1], "federated", keys.Federated},
		{"explicit cross read uses reader", topo.private[0], "cross_read", keys.Reader},
		{"another agent's shared namespace uses reader", topo.shared[1], "dense", keys.Reader},
		{"owned private namespace uses writer", topo.private[0], "dense", keys.Writer},
		{"common namespace uses writer", topo.common, "insert", keys.Writer},
		{"unknown op respects read-only namespace", topo.shared[1], "custom", keys.Reader},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a.setKeyFor(c, tc.ns, tc.op)
			if got := authBearer(c); got != tc.want {
				t.Fatalf("credential = %q, want %q", got, tc.want)
			}
		})
	}

	cfg.Agents, cfg.AgentsPerNamespace = 8, 2
	grouped := newTopologyUnchecked(cfg)
	a.topo = grouped
	a.id = 1
	groupedCases := []struct {
		ns, op, want string
	}{
		{"shared_0", "dense", keys.Writer},
		{"shared_1", "dense", keys.Reader},
		{"master", "insert", keys.Writer},
		{"shared_1", "federated", keys.Federated},
	}
	for _, tc := range groupedCases {
		a.setKeyFor(c, tc.ns, tc.op)
		if got := authBearer(c); got != tc.want {
			t.Fatalf("grouped %s/%s credential = %q, want %q", tc.ns, tc.op, got, tc.want)
		}
	}
}

// TestApplyOIDCModeValidation covers the credential exchange validation that
// runs without a server: disabled modes are no-ops, and bad AWS token sources
// fail before any soak operation is attempted.
func TestApplyOIDCModeValidation(t *testing.T) {
	cfg := defaultConfig()
	for _, mode := range []string{"none", ""} {
		cfg.Security.OIDC.Mode = mode
		if err := applyOIDC(context.Background(), nil, cfg); err != nil {
			t.Fatalf("mode %q must be a no-op: %v", mode, err)
		}
	}
	cfg.Security.OIDC.Mode = "unsupported"
	if err := applyOIDC(context.Background(), nil, cfg); err == nil {
		t.Fatal("expected error for unsupported OIDC mode")
	}

	env := "LONGBOW_TEST_OIDC_TOKEN_FILE"
	t.Setenv(env, "")
	cfg.Security.OIDC.Mode = "aws"
	cfg.Security.OIDC.TokenFileEnv = env
	if err := applyOIDC(context.Background(), nil, cfg); err == nil || !strings.Contains(err.Error(), env) {
		t.Fatalf("missing token file error = %v, want env name %q", err, env)
	}

	path := filepath.Join(t.TempDir(), "token.jwt")
	if err := os.WriteFile(path, []byte("  \n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(env, path)
	if err := applyOIDC(context.Background(), nil, cfg); err == nil || !strings.Contains(err.Error(), "empty token") {
		t.Fatalf("blank token error = %v, want empty token", err)
	}
}

// TestAgentRunReturnsOnShutdown verifies that an agent handed an already
// cancelled context exits without recording in-flight shutdown failures.
func TestAgentRunReturnsOnShutdown(t *testing.T) {
	cfg := defaultConfig()
	cfg.Dim = 4
	cfg.Workload = map[string]float64{"dense": 1}
	topo := newTopologyUnchecked(cfg)
	selector, err := newWeightedSelector(cfg.Workload)
	if err != nil {
		t.Fatal(err)
	}
	gm := newGlobalMetrics()
	var stop atomic.Bool
	a := &agent{
		id: 0, cfg: cfg, topo: topo,
		keys:     agentKeys{Writer: "writer-key"},
		metrics:  gm,
		stop:     &stop,
		selector: selector,
		ids:      make(map[string][]string),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		a.run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not exit after shutdown")
	}
	if gm.Ops.Load() != 0 || gm.Errors.Load() != 0 || len(gm.ByOp) != 0 {
		t.Fatalf("shutdown recorded operations: ops=%d errors=%d by_op=%v",
			gm.Ops.Load(), gm.Errors.Load(), gm.ByOp)
	}
	if a.verified.Load() != 0 {
		t.Fatal("shutdown recorded cross-agent verification")
	}
}

// TestRetryIDNotFoundStopsAtDeadline keeps the async-ingest retry from outliving
// the bounded soak or swallowing cancellation while waiting.
func TestRetryIDNotFoundStopsAtDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := retryIDNotFound(ctx, func() error {
		return status.Error(codes.NotFound, "not indexed yet")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("retry ignored deadline: elapsed %v", elapsed)
	}

	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := retryIDNotFound(ctx, func() error {
		return status.Error(codes.NotFound, "not indexed yet")
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v, want Canceled", err)
	}
}

// soakReportSnapshot is the subset of a soak report consumed by the Go strict
// gate and by scripts/agent_soak_bench.py. It is deliberately permissive about
// stalls formats so fuzzing can cover every shape emitted by the simulators.
type soakReportSnapshot struct {
	QPS           float64
	ErrorRate     float64
	P99MS         float64
	Ops           float64
	Errors        float64
	CrossVerified float64
	ByOp          map[string]map[string]float64
	Stalls        map[string]int
}

func reportNumber(value any, label string) (float64, error) {
	n, ok := value.(float64)
	if !ok {
		return 0, fmt.Errorf("%s must be numeric", label)
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("%s must be finite", label)
	}
	return n, nil
}

// parseSoakReport mirrors the report contract of the collation script so unit
// and fuzz tests can reject reports that would break long soak analysis.
func parseSoakReport(data []byte) (*soakReportSnapshot, error) {
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	raw, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("report must be a JSON object")
	}
	out := &soakReportSnapshot{
		ByOp:   make(map[string]map[string]float64),
		Stalls: make(map[string]int),
	}
	fields := []struct {
		key string
		dst *float64
	}{
		{"qps", &out.QPS}, {"error_rate", &out.ErrorRate}, {"p99_ms", &out.P99MS},
		{"ops", &out.Ops}, {"errors", &out.Errors}, {"cross_verified", &out.CrossVerified},
	}
	for _, field := range fields {
		value, ok := raw[field.key]
		if !ok {
			if field.key == "cross_verified" {
				continue
			}
			return nil, fmt.Errorf("missing required field %q", field.key)
		}
		n, err := reportNumber(value, field.key)
		if err != nil {
			return nil, err
		}
		*field.dst = n
	}

	byOp := raw["by_op_stats"]
	if byOp == nil {
		byOp = raw["by_op"]
	}
	if byOp != nil {
		ops, ok := byOp.(map[string]any)
		if !ok {
			return nil, errors.New("by_op_stats must be an object")
		}
		for op, value := range ops {
			var src map[string]any
			switch v := value.(type) {
			case map[string]any:
				src = v
			case float64:
				n, err := reportNumber(v, "by_op_stats["+op+"]")
				if err != nil {
					return nil, err
				}
				src = map[string]any{"ops": n}
			default:
				return nil, fmt.Errorf("by_op_stats[%q] must be an object", op)
			}
			stats := make(map[string]float64)
			for _, key := range []string{"ops", "errors", "error_rate", "qps", "p99_ms", "stalls"} {
				value, exists := src[key]
				if !exists {
					continue
				}
				n, err := reportNumber(value, "by_op_stats["+op+"]."+key)
				if err != nil {
					return nil, err
				}
				stats[key] = n
			}
			out.ByOp[op] = stats
		}
	}

	switch stalls := raw["stalls"].(type) {
	case nil:
	case float64:
		n, err := reportNumber(stalls, "stalls")
		if err != nil {
			return nil, err
		}
		if n != 0 {
			out.Stalls["total"] = int(n)
		}
	case map[string]any:
		for op, value := range stalls {
			n, err := reportNumber(value, "stalls["+op+"]")
			if err != nil {
				return nil, err
			}
			out.Stalls[op] = int(n)
		}
	case []any:
		for _, item := range stalls {
			entry, ok := item.(map[string]any)
			if !ok {
				return nil, errors.New("stalls entries must be objects")
			}
			op, ok := entry["op"].(string)
			if !ok || op == "" {
				return nil, errors.New("stalls entries must have a string op")
			}
			out.Stalls[op]++
		}
	default:
		return nil, errors.New("stalls must be a list, object, or number")
	}
	return out, nil
}

// reportSnapshot mirrors main()'s final report assembly so tests can assert
// the schema consumed by scripts/check_soak_report.py and the collation script.
func reportSnapshot(gm *globalMetrics, cfg *soakConfig, topo *topology, verified int64, failures []string, finished time.Time) map[string]any {
	gm.mu.Lock()
	lats := append([]float64{}, gm.Lats...)
	gm.mu.Unlock()
	sort.Float64s(lats)
	pct := func(p float64) float64 { return percentileMS(lats, p) }
	errorRate, qps := float64(0), float64(0)
	if ops := gm.Ops.Load(); ops > 0 {
		errorRate = float64(gm.Errors.Load()) / float64(ops)
		qps = float64(ops) / finished.Sub(gm.Start).Seconds()
	}
	return map[string]any{
		"agents":           cfg.Agents,
		"namespaces":       len(topo.Namespaces()),
		"namespace_layout": topo.Namespaces(),
		"rbac_enforced":    true,
		"security_variant": cfg.Security.Variant,
		"workload":         cfg.Workload,
		"duration_s":       finished.Sub(gm.Start).Seconds(),
		"ops":              gm.Ops.Load(),
		"errors":           gm.Errors.Load(),
		"error_rate":       errorRate,
		"qps":              qps,
		"p50_ms":           pct(50),
		"p95_ms":           pct(95),
		"p99_ms":           pct(99),
		"cross_verified":   verified,
		"by_op":            gm.byOpCounts(),
		"by_op_errors":     gm.ByErr,
		"by_op_stats":      gm.byOpStats(finished),
		"stalls":           gm.stalls(finished),
		"audit_denials":    map[string]int{},
		"failures":         failures,
	}
}

// TestReportGenerationHasStrictGateFields pins the report fields consumed by
// the strict soak gate and by the Python collation script.
func TestReportGenerationHasStrictGateFields(t *testing.T) {
	cfg := defaultConfig()
	topo := newTopologyUnchecked(cfg)
	gm := newGlobalMetrics()
	finished := time.Now()
	gm.Start = finished.Add(-100 * time.Second)
	for i := 0; i < 2; i++ {
		gm.record("dense", 0.01, true)
	}
	gm.record("dense", 0.02, false)
	gm.record("federated", 0.03, true)
	federated := gm.ByOp["federated"]
	federated.Windows = map[int]int64{0: 60, 1: 60, 2: 5, 3: 5}
	federated.Last = finished

	failures := []string{"error_rate 0.2500 exceeds 1%", "1 operation stalls detected"}
	report := reportSnapshot(gm, cfg, topo, 2, failures, finished)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseSoakReport(data)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Ops != 4 || parsed.Errors != 1 || math.Abs(parsed.ErrorRate-0.25) > 1e-9 {
		t.Fatalf("gate totals = %+v", parsed)
	}
	if parsed.QPS <= 0 || parsed.P99MS <= 0 || parsed.CrossVerified != 2 {
		t.Fatalf("missing report metrics: %+v", parsed)
	}
	if rate := parsed.ByOp["dense"]["error_rate"]; math.Abs(rate-1.0/3.0) > 1e-9 {
		t.Fatalf("dense error_rate = %v, want 1/3", rate)
	}
	if len(parsed.Stalls) != 1 || parsed.Stalls["federated"] != 1 || parsed.ByOp["federated"]["stalls"] != 0 {
		t.Fatalf("stalls = %v, by_op = %v", parsed.Stalls, parsed.ByOp)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"agents", "namespaces", "namespace_layout", "rbac_enforced",
		"security_variant", "workload", "duration_s", "ops", "errors",
		"error_rate", "qps", "p50_ms", "p95_ms", "p99_ms", "cross_verified",
		"by_op", "by_op_errors", "by_op_stats", "stalls", "audit_denials", "failures",
	} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("report is missing field %q", key)
		}
	}
	reportedFailures, ok := raw["failures"].([]any)
	if !ok || len(reportedFailures) != len(failures) {
		t.Fatalf("report failures = %v, want %v", raw["failures"], failures)
	}
}

// TestParseSoakReportRejectsMalformed covers the negative cases fuzz seeds grow
// from: non-objects, missing fields, non-numeric values, and bad stall entries.
func TestParseSoakReportRejectsMalformed(t *testing.T) {
	cases := []struct {
		name, report, wantErr string
	}{
		{"non object", `[]`, "JSON object"},
		{"null", `null`, "JSON object"},
		{"missing fields", `{}`, "missing required field"},
		{"non numeric field", `{"qps":"fast","error_rate":0,"p99_ms":1,"ops":1,"errors":0}`, "must be numeric"},
		{"non finite field", `{"qps":1e400,"error_rate":0,"p99_ms":1,"ops":1,"errors":0}`, ""},
		{"bad by op", `{"qps":1,"error_rate":0,"p99_ms":1,"ops":1,"errors":0,"by_op_stats":["dense"]}`, "by_op_stats must be an object"},
		{"bad stall entry", `{"qps":1,"error_rate":0,"p99_ms":1,"ops":1,"errors":0,"stalls":[3]}`, "stalls entries"},
		{"bad stall op", `{"qps":1,"error_rate":0,"p99_ms":1,"ops":1,"errors":0,"stalls":[{"op":["dense"]}]}`, "stalls entries"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSoakReport([]byte(tc.report))
			if err == nil {
				t.Fatal("expected parse error")
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantErr)
			}
		})
	}
}

// FuzzSoakReportParsing exercises report parsing with the mixed stall and
// per-op formats emitted by the Go and Python soak simulators.
func FuzzSoakReportParsing(f *testing.F) {
	f.Add([]byte(`{"qps":10,"error_rate":0.01,"p99_ms":12,"ops":100,"errors":1,"cross_verified":3,"by_op_stats":{"dense":{"ops":50,"error_rate":0,"qps":5,"p99_ms":10}},"stalls":[{"op":"dense","drop_pct":60}]}`))
	f.Add([]byte(`{"qps":1,"error_rate":0,"p99_ms":2,"ops":10,"errors":0,"by_op":{"dense":10},"stalls":2}`))
	f.Add([]byte(`{"qps":1,"error_rate":0,"p99_ms":2,"ops":10,"errors":0,"stalls":{"dense":1,"sparse":3}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"qps":"fast","error_rate":0,"p99_ms":1,"ops":1,"errors":0}`))
	f.Add([]byte(`{"qps":1,"error_rate":0,"p99_ms":1,"ops":1,"errors":0,"stalls":[{"op":["dense"]}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			return
		}
		report, err := parseSoakReport(data)
		if err != nil {
			return
		}
		for _, value := range []float64{
			report.QPS, report.ErrorRate, report.P99MS,
			report.Ops, report.Errors, report.CrossVerified,
		} {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				t.Fatalf("non-finite or negative report field: %v", value)
			}
		}
		for op, stats := range report.ByOp {
			for key, value := range stats {
				if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
					t.Fatalf("by_op_stats[%q].%s = %v", op, key, value)
				}
			}
		}
		for op, count := range report.Stalls {
			if count < 0 {
				t.Fatalf("stall count for %q = %d", op, count)
			}
		}
	})
}
