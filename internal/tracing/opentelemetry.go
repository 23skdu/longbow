package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// TraceSpan represents a trace span value.
type TraceSpan struct {
	span oteltrace.Span
}

// Tracer represents a tracer value.
type Tracer struct {
}

// SpanConfig represents a span config value.
type SpanConfig struct {
	ServiceName    string
	ServiceVersion string
	SampleRate     float64
	TraceExport    bool
}

var globalTracer oteltrace.Tracer

// InitTracer runs the init tracer operation.
func InitTracer(config SpanConfig) error {
	if config.SampleRate < 0 || config.SampleRate > 1 {
		return fmt.Errorf("sample rate must be between 0 and 1")
	}

	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return fmt.Errorf("failed to create stdout exporter: %w", err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(config.ServiceName),
			semconv.ServiceVersionKey.String(config.ServiceVersion),
		)),
		trace.WithSampler(trace.TraceIDRatioBased(config.SampleRate)),
	)

	otel.SetTracerProvider(tp)
	globalTracer = tp.Tracer("longbow")
	return nil
}

// CreateSpan runs the create span operation.
func CreateSpan(ctx context.Context, name string) (context.Context, *TraceSpan) {
	if globalTracer == nil {
		return ctx, nil
	}

	newCtx, span := globalTracer.Start(ctx, name, oteltrace.WithAttributes(
		semconv.ServiceInstanceIDKey.String("tracing"),
	))

	return newCtx, &TraceSpan{
		span: span,
	}
}

// End runs the end operation.
func (s *TraceSpan) End() {
	if s != nil && s.span != nil {
		s.span.End()
	}
}

// SetStatus sets the status.
func (s *TraceSpan) SetStatus(code codes.Code, description string) {
	if s != nil && s.span != nil {
		s.span.SetStatus(code, description)
	}
}

// SetError sets the error.
func (s *TraceSpan) SetError(err error) {
	if s != nil && s.span != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	}
}

// SetAttributes sets the attributes.
func (s *TraceSpan) SetAttributes(keyValues ...string) {
	if s == nil || s.span == nil {
		return
	}
	var attrs []attribute.KeyValue
	for i := 0; i < len(keyValues)-1; i += 2 {
		attrs = append(attrs, attribute.String(keyValues[i], keyValues[i+1]))
	}
	s.span.SetAttributes(attrs...)
}

// GetTraceID returns the trace id.
func (s *TraceSpan) GetTraceID() string {
	if s == nil || s.span == nil {
		return ""
	}
	return s.span.SpanContext().TraceID().String()
}

// GetContextTraceID returns the context trace id.
func GetContextTraceID(ctx context.Context) string {
	span := oteltrace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}

// SpanAttributeKey represents a span attribute key value.
type SpanAttributeKey string

const (
	// ComponentKey is a component key constant.
	ComponentKey SpanAttributeKey = "component"
	// LevelKey is a level key constant.
	LevelKey SpanAttributeKey = "level"
	// ErrorKey is a error key constant.
	ErrorKey SpanAttributeKey = "error"
	// TraceIDKey is a trace id key constant.
	TraceIDKey SpanAttributeKey = "trace_id"
	// SpanIDKey is a span id key constant.
	SpanIDKey SpanAttributeKey = "span_id"
)
