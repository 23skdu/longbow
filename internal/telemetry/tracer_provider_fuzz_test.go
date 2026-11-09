package telemetry

import (
	"context"
	"strings"
	"testing"
)

// FuzzInitTracerProvider verifies that tracer-provider construction accepts
// arbitrary (bounded) service metadata: it must never fail or panic for any
// service name/version, and the returned provider must shut down cleanly.
// The environment is pinned so the OTLP (network) and stdout exporter paths
// are never taken and no exporter goroutines are spawned.
func FuzzInitTracerProvider(f *testing.F) {
	// Valid seeds.
	f.Add("test-service", "1.0.0")
	f.Add("svc with spaces", "v1.2.3-rc.1+build.5")
	f.Add("λ-service", "版本 1.0")
	f.Add(strings.Repeat("a", 300), "0.0.0")
	// Invalid-but-accepted seeds: empty values and control characters.
	f.Add("", "")
	f.Add("svc\nwith\nnewlines", "ver\r\nwith\r\nnewlines")

	f.Fuzz(func(t *testing.T, serviceName, serviceVersion string) {
		// Bound: skip inputs outside the supported size range.
		if len(serviceName) > 512 || len(serviceVersion) > 512 {
			return
		}

		// Pin exporter configuration: no OTLP endpoint and no debug
		// exporter, so InitTracerProvider neither dials out nor starts
		// background exporter goroutines.
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
		t.Setenv("LONGBOW_DEBUG", "")

		ctx := context.Background()
		tp, err := InitTracerProvider(ctx, serviceName, serviceVersion)
		if err != nil {
			t.Fatalf("InitTracerProvider(%q, %q) failed: %v", serviceName, serviceVersion, err)
		}
		if tp == nil {
			t.Fatalf("InitTracerProvider(%q, %q) returned nil tracer provider", serviceName, serviceVersion)
		}
		if err := tp.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown after InitTracerProvider(%q, %q) failed: %v", serviceName, serviceVersion, err)
		}
	})
}
