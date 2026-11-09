package tracing

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// FuzzInitTracerSampleRate checks the SampleRate validation contract of
// InitTracer: values outside [0,1] must be rejected with an error, values
// inside the range must be accepted. When a provider is created, its batch
// exporter goroutine is shut down before the iteration returns.
func FuzzInitTracerSampleRate(f *testing.F) {
	// Valid seeds.
	f.Add("svc", "1.0.0", 0.0)
	f.Add("svc", "1.0.0", 0.5)
	f.Add("svc", "1.0.0", 1.0)
	f.Add("svc\nwith\nnl", "ver\r\nwith\r\nnl", 0.99) // control chars in names
	// Invalid seeds: sample rate out of range.
	f.Add("", "", -0.5)
	f.Add("svc", "v2", 1.5)

	f.Fuzz(func(t *testing.T, serviceName, serviceVersion string, sampleRate float64) {
		// Bound: skip inputs outside the supported ranges.
		if len(serviceName) > 512 || len(serviceVersion) > 512 {
			return
		}
		if math.IsNaN(sampleRate) {
			return // NaN is outside the representable [0, 1] range
		}

		err := InitTracer(SpanConfig{
			ServiceName:    serviceName,
			ServiceVersion: serviceVersion,
			SampleRate:     sampleRate,
		})

		if sampleRate < 0 || sampleRate > 1 {
			if err == nil {
				t.Fatalf("InitTracer accepted out-of-range sample rate %v", sampleRate)
			}
			return
		}
		if err != nil {
			t.Fatalf("InitTracer rejected in-range sample rate %v: %v", sampleRate, err)
		}

		// InitTracer installed a batch exporter backed by a background
		// goroutine; shut the provider down so the goroutine is joined
		// before this iteration returns.
		if tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
			if err := tp.Shutdown(context.Background()); err != nil {
				t.Fatalf("shutting down tracer provider failed: %v", err)
			}
		}
	})
}

// FuzzTraceSpanAttributes exercises span status/error/attribute handling with
// arbitrary payloads. Invariants: trace IDs are well-formed 32-char lowercase
// hex, the ID observed through the span's context matches the span's own ID,
// odd (unpaired) attribute lists and control characters are tolerated, and a
// nil receiver stays safe.
func FuzzTraceSpanAttributes(f *testing.F) {
	// Valid seeds.
	f.Add("op", "component", "api", "failed")
	f.Add("", "", "", "")
	f.Add(strings.Repeat("o", 300), strings.Repeat("k", 300), strings.Repeat("v", 300), strings.Repeat("d", 300))
	// Invalid/edge seeds: control characters, NULs, unpaired attribute lists.
	f.Add("op\nwith\nnl", "key\n", "value\r\n", "desc\nwith\r\n")
	f.Add("op", "k\x00ey", "v\x00al", "\x00")
	f.Add("op", "unpaired-key", "", "")

	f.Fuzz(func(t *testing.T, name, key, value, desc string) {
		// Bound: skip unreasonably large payloads.
		if len(name) > 512 || len(key) > 512 || len(value) > 512 || len(desc) > 512 {
			return
		}

		// A provider without exporters creates no background goroutines.
		tp := sdktrace.NewTracerProvider()
		defer func() { _ = tp.Shutdown(context.Background()) }()

		ctx, sp := tp.Tracer("fuzz").Start(context.Background(), name)
		span := &TraceSpan{span: sp}

		// Trace IDs must be well-formed 32-char lowercase hex.
		id := span.GetTraceID()
		if len(id) != 32 {
			t.Fatalf("GetTraceID() = %q, want 32 hex characters", id)
		}
		for _, c := range id {
			if !strings.ContainsRune("0123456789abcdef", c) {
				t.Fatalf("GetTraceID() = %q contains non-hex character %q", id, c)
			}
		}
		// The trace ID observed through the span's context must match.
		if got := GetContextTraceID(ctx); got != id {
			t.Fatalf("GetContextTraceID(ctx) = %q, span trace ID = %q", got, id)
		}

		// Status, error and attributes must tolerate arbitrary payloads,
		// including control characters and unpaired key/value lists.
		span.SetAttributes(key, value)
		span.SetAttributes(key) // odd length: last key is dropped
		span.SetStatus(codes.Error, desc)
		span.SetError(errors.New(desc))
		span.End()
		span.End() // double End must stay safe

		// A nil receiver must remain safe for arbitrary payloads as well.
		var nilSpan *TraceSpan
		nilSpan.SetAttributes(key, value)
		nilSpan.SetStatus(codes.Ok, desc)
		nilSpan.SetError(errors.New(desc))
		nilSpan.End()
		if got := nilSpan.GetTraceID(); got != "" {
			t.Fatalf("nil TraceSpan.GetTraceID() = %q, want empty", got)
		}
	})
}
