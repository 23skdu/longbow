package telemetry

import (
	"context"
	"testing"
)

func TestInitTracerProvider(t *testing.T) {
	ctx := context.Background()

	tp, err := InitTracerProvider(ctx, "test-service", "1.0.0")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if tp == nil {
		t.Error("expected non-nil tracer provider")
	}

	// Shutdown
	if err := tp.Shutdown(ctx); err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}
}

func TestInitTracerProvider_WithOTLP(t *testing.T) {
	// Set OTLP endpoint (this will try to connect, may fail in test env)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")

	ctx := context.Background()
	tp, err := InitTracerProvider(ctx, "test-service", "1.0.0")
	if err != nil {
		// Connection may fail, which is ok
		t.Logf("OTLP connection error (expected in test env): %v", err)
		return
	}
	if tp == nil {
		t.Error("expected non-nil tracer provider")
	}

	// Shutdown
	if err := tp.Shutdown(ctx); err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}
}

func TestInitTracerProvider_WithDebug(t *testing.T) {
	t.Setenv("LONGBOW_DEBUG", "true")

	ctx := context.Background()
	tp, err := InitTracerProvider(ctx, "test-service", "1.0.0")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if tp == nil {
		t.Error("expected non-nil tracer provider")
	}

	// Shutdown
	if err := tp.Shutdown(ctx); err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}
}
