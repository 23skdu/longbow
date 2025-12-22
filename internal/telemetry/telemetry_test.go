package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInitTracerProvider_Stdout(t *testing.T) {
	// Reset once for testing? Accessing private var is tricky.
	// We can't easily reset sync.Once.
	// So we might only really test this once per process execution.
	// Or we just verify it doesn't crash.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
		UseStdout:      true,
		SampleRatio:    1.0,
	}

	shutdown, err := InitTracerProvider(ctx, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, shutdown)

	// Verify tracer
	tracer := Tracer("test")
	assert.NotNil(t, tracer)

	ctx, span := tracer.Start(ctx, "test-span")
	span.End()

	err = shutdown(ctx)
	assert.NoError(t, err)
}
