package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/codes"
)

func TestInitTracer(t *testing.T) {
	err := InitTracer(SpanConfig{
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		SampleRate:     0.5,
		TraceExport:    false,
	})
	assert.NoError(t, err)
}

func TestInitTracerInvalidSampleRate(t *testing.T) {
	err := InitTracer(SpanConfig{SampleRate: -1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sample rate")

	err = InitTracer(SpanConfig{SampleRate: 1.5})
	assert.Error(t, err)
}

func TestCreateSpan(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     0.0,
	})
	ctx, span := CreateSpan(context.Background(), "test-operation")
	assert.NotNil(t, ctx)

	if span != nil {
		assert.NotPanics(t, span.End)
	}
}

func TestCreateSpanNoTracer(t *testing.T) {
	globalTracer = nil
	ctx, span := CreateSpan(context.Background(), "test")
	assert.NotNil(t, ctx)
	assert.Nil(t, span)
}

func TestTraceSpanEnd(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     1.0,
	})
	_, span := CreateSpan(context.Background(), "op")
	span.End()
	assert.NotPanics(t, span.End)
}

func TestTraceSpanNilSafety(t *testing.T) {
	var nilSpan *TraceSpan
	assert.NotPanics(t, nilSpan.End)
	assert.NotPanics(t, func() { nilSpan.SetStatus(codes.Ok, "") })
	assert.NotPanics(t, func() { nilSpan.SetError(nil) })
	assert.NotPanics(t, func() { nilSpan.SetAttributes() })
	assert.Equal(t, "", nilSpan.GetTraceID())
}

func TestTraceSpanSetStatus(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     1.0,
	})
	_, span := CreateSpan(context.Background(), "op")
	assert.NotPanics(t, func() { span.SetStatus(codes.Error, "something went wrong") })
}

func TestTraceSpanSetError(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     1.0,
	})
	_, span := CreateSpan(context.Background(), "op")
	assert.NotPanics(t, func() { span.SetError(assert.AnError) })
}

func TestTraceSpanSetAttributes(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     1.0,
	})
	_, span := CreateSpan(context.Background(), "op")
	assert.NotPanics(t, func() { span.SetAttributes("key1", "val1", "key2", "val2") })
}

func TestTraceSpanGetTraceID(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     1.0,
	})
	_, span := CreateSpan(context.Background(), "op")
	id := span.GetTraceID()
	assert.IsType(t, "", id)
}

func TestGetContextTraceID(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     1.0,
	})
	ctx := context.Background()
	id := GetContextTraceID(ctx)
	assert.NotPanics(t, func() { _ = id })
	assert.Empty(t, id)
}

func TestGetContextTraceIDWithSpan(t *testing.T) {
	InitTracer(SpanConfig{
		ServiceName:    "test",
		ServiceVersion: "1.0",
		SampleRate:     1.0,
	})
	ctx, span := CreateSpan(context.Background(), "test")
	if span != nil {
		id := GetContextTraceID(ctx)
		assert.NotPanics(t, func() { _ = id })
		span.End()
	}
}

func TestGetContextTraceIDNoTracer(t *testing.T) {
	globalTracer = nil
	ctx := context.Background()
	id := GetContextTraceID(ctx)
	assert.Empty(t, id)
}

func TestSpanAttributeKeys(t *testing.T) {
	assert.Equal(t, SpanAttributeKey("component"), ComponentKey)
	assert.Equal(t, SpanAttributeKey("level"), LevelKey)
	assert.Equal(t, SpanAttributeKey("error"), ErrorKey)
	assert.Equal(t, SpanAttributeKey("trace_id"), TraceIDKey)
	assert.Equal(t, SpanAttributeKey("span_id"), SpanIDKey)
}
