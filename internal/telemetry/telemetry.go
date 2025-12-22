package telemetry

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	tp   *sdktrace.TracerProvider
	once sync.Once
)

// Config holds configuration for telemetry.
type Config struct {
	ServiceName    string
	ServiceVersion string
	Endpoint       string // OTLP endpoint (e.g. "localhost:4317")
	UseStdout      bool   // If true, use stdout exporter (dev mode)
	SampleRatio    float64
}

// InitTracerProvider initializes the global trace provider.
// It returns a shutdown function that should be called when the service terminates.
func InitTracerProvider(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	var err error
	once.Do(func() {
		var exporter sdktrace.SpanExporter

		if cfg.UseStdout {
			exporter, err = stdouttrace.New(
				stdouttrace.WithPrettyPrint(),
			)
		} else if cfg.Endpoint != "" {
			exporter, err = otlptracegrpc.New(ctx,
				otlptracegrpc.WithInsecure(), // TODO: Support TLS
				otlptracegrpc.WithEndpoint(cfg.Endpoint),
			)
		} else {
			// No-op or stdout fallback if nothing specified?
			// For now, let's default to no-op (nil exporter implies no export) unless Stdout requested.
			// But sdktrace.NewBatchSpanProcessor panics on nil.
			// Return no-op cleanup if no config.
			return
		}

		if err != nil {
			err = fmt.Errorf("failed to create exporter: %w", err)
			return
		}

		res, rErr := resource.New(ctx,
			resource.WithAttributes(
				semconv.ServiceName(cfg.ServiceName),
				semconv.ServiceVersion(cfg.ServiceVersion),
			),
		)
		if rErr != nil {
			err = fmt.Errorf("failed to create resource: %w", rErr)
			return
		}

		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		)

		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	})

	if err != nil {
		return nil, err
	}

	if tp == nil {
		return func(_ context.Context) error { return nil }, nil
	}

	return tp.Shutdown, nil
}

// Tracer returns a named tracer.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
