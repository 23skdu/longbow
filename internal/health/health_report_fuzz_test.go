package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"unicode/utf8"
)

// fuzzCheckerNames is a fixed, low-cardinality set of component names. The
// name ends up as a Prometheus label value on the package-level collectors
// shared by every HealthManager, so it must not be fuzzed directly or long
// fuzzing runs would grow the collector set without bound.
var fuzzCheckerNames = []string{"comp-alpha", "comp-beta", "comp-gamma"}

// fuzzChecker reports a status and message supplied by the fuzz input. It
// performs no IO, sleeps, or goroutine work.
type fuzzChecker struct {
	name    string
	status  HealthStatus
	message string
}

func (c *fuzzChecker) Name() string { return c.name }

func (c *fuzzChecker) Check(_ context.Context) *ComponentHealth {
	return &ComponentHealth{
		Name:        c.name,
		Status:      c.status,
		Message:     c.message,
		LastChecked: time.Now(),
		Metadata:    map[string]interface{}{"fuzz": c.message},
	}
}

// FuzzHealthReport checks status aggregation, JSON round-tripping and the
// HTTP status mapping of the health report for arbitrary component inputs.
func FuzzHealthReport(f *testing.F) {
	f.Add("1.0.0", "healthy", "all good", 0)
	f.Add("1.0.0", "degraded", "slow disk", 1)
	f.Add("1.0.0", "unhealthy", "down\nnow", 2)
	f.Add("", "weird-status", "", 0)
	f.Add("2.0.0-rc1", "UNHEALTHY", "quote \" and ctrl \x01", 1)
	f.Add("v1", "degraded", "unicode \u2028 line", 2)

	f.Fuzz(func(t *testing.T, version, status, message string, selector int) {
		// Bound the inputs: the component name comes from a fixed set and
		// the strings must round-trip through JSON verbatim.
		if selector < 0 || selector >= len(fuzzCheckerNames) {
			return
		}
		if !utf8.ValidString(version) || !utf8.ValidString(status) || !utf8.ValidString(message) {
			return
		}
		if len(version) > 128 || len(status) > 32 || len(message) > 512 {
			return
		}

		hm := NewHealthManager(version, makeLogger(), makeTracer())
		hm.RegisterChecker(&fuzzChecker{
			name:    fuzzCheckerNames[selector],
			status:  HealthStatus(status),
			message: message,
		})

		health := hm.CheckHealth(context.Background())
		if health == nil {
			t.Fatal("CheckHealth returned nil")
		}

		// Overall status: the component status when it is one of the known
		// values, otherwise healthy (an unrecognized status cannot degrade
		// the system).
		want := StatusHealthy
		switch HealthStatus(status) {
		case StatusHealthy, StatusDegraded, StatusUnhealthy:
			want = HealthStatus(status)
		}
		if health.Status != want {
			t.Errorf("overall status = %q, want %q (component status %q)", health.Status, want, status)
		}
		if health.Version != version {
			t.Errorf("version = %q, want %q", health.Version, version)
		}
		if health.CheckCount != 1 {
			t.Errorf("check count = %d, want 1", health.CheckCount)
		}

		name := fuzzCheckerNames[selector]
		comp, ok := health.Components[name]
		if !ok {
			t.Fatalf("component %q missing from report", name)
		}
		if comp.Status != HealthStatus(status) {
			t.Errorf("component status = %q, want %q", comp.Status, status)
		}
		if comp.Message != message {
			t.Errorf("component message = %q, want %q", comp.Message, message)
		}

		// The report must survive a JSON round trip unchanged.
		raw, err := json.Marshal(health)
		if err != nil {
			t.Fatalf("marshal report: %v", err)
		}
		var back SystemHealth
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("unmarshal report: %v", err)
		}
		if back.Status != health.Status || back.Version != health.Version || back.CheckCount != health.CheckCount {
			t.Errorf("round trip mismatch: got status=%q version=%q count=%d",
				back.Status, back.Version, back.CheckCount)
		}
		backComp := back.Components[name]
		if backComp == nil {
			t.Fatalf("component %q lost in round trip", name)
		}
		if backComp.Status != comp.Status || backComp.Message != comp.Message {
			t.Errorf("round trip component = %+v, want %+v", backComp, comp)
		}
		if got := backComp.Metadata["fuzz"]; got != message {
			t.Errorf("metadata round trip = %v, want %q", got, message)
		}

		// The HTTP handler must map the aggregated status onto the right
		// response code and serve the same report.
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		hm.HTTPHandler().ServeHTTP(rec, req)
		wantCode := http.StatusOK
		if want == StatusUnhealthy {
			wantCode = http.StatusServiceUnavailable
		}
		if rec.Code != wantCode {
			t.Errorf("HTTP status = %d, want %d", rec.Code, wantCode)
		}
		var viaHTTP SystemHealth
		if err := json.NewDecoder(rec.Body).Decode(&viaHTTP); err != nil {
			t.Fatalf("decode HTTP body: %v", err)
		}
		if viaHTTP.Status != want {
			t.Errorf("HTTP report status = %q, want %q", viaHTTP.Status, want)
		}
		if viaHTTP.Version != version {
			t.Errorf("HTTP report version = %q, want %q", viaHTTP.Version, version)
		}
	})
}
