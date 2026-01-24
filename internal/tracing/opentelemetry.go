package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type TraceSpan struct {
	span oteltrace.Span
}

type Tracer struct {
	provider oteltrace.TracerProvider
	tracer   oteltrace.Tracer
}

type SpanConfig struct {
	ServiceName    string
	ServiceVersion string
	SampleRate     float64
	TraceExport    bool
}

var globalTracer oteltrace.Tracer
var traceConfig SpanConfig

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
	traceConfig = config
	return nil
}

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

func (s *TraceSpan) End() {
	if s != nil && s.span != nil {
		s.span.End()
	}
}

func (s *TraceSpan) SetStatus(code codes.Code, description string) {
	if s != nil && s.span != nil {
		s.span.SetStatus(code, description)
	}
}

func (s *TraceSpan) SetError(err error) {
	if s != nil && s.span != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	}
}

func (s *TraceSpan) GetTraceID() string {
	if s == nil || s.span == nil {
		return ""
	}
	return s.span.SpanContext().TraceID().String()
}

func GetContextTraceID(ctx context.Context) string {
	span := oteltrace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}

type SpanAttributeKey string

const (
	ComponentKey SpanAttributeKey = "component"
	LevelKey     SpanAttributeKey = "level"
	ErrorKey     SpanAttributeKey = "error"
	TraceIDKey   SpanAttributeKey = "trace_id"
	SpanIDKey    SpanAttributeKey = "span_id"
)
