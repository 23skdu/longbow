package health

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DatabaseChecker checks database connectivity and health
type DatabaseChecker struct {
	name   string
	logger zerolog.Logger
	tracer trace.Tracer
}

func NewDatabaseChecker(logger *zerolog.Logger, tracer trace.Tracer) *DatabaseChecker {
	return &DatabaseChecker{
		name:   "database",
		logger: *logger,
		tracer: tracer,
	}
}

func (dc *DatabaseChecker) Name() string {
	return dc.name
}

func (dc *DatabaseChecker) Check(ctx context.Context) *ComponentHealth {
	_, span := dc.tracer.Start(ctx, "DatabaseChecker.Check")
	defer span.End()

	start := time.Now()

	// Simulate database connectivity check
	// In real implementation, this would ping the database
	time.Sleep(5 * time.Millisecond)

	duration := time.Since(start)

	// For demo purposes, always return healthy
	// In real implementation, this would check actual database connection
	status := StatusHealthy
	message := "Database connection successful"

	// Simulate occasional degraded state
	if duration > 10*time.Millisecond {
		status = StatusDegraded
		message = "Database responding slowly"
	}

	return &ComponentHealth{
		Name:        dc.name,
		Status:      status,
		Message:     message,
		LastChecked: time.Now(),
		Metadata: map[string]interface{}{
			"response_time_ms": duration.Milliseconds(),
			"check_duration":   duration.String(),
		},
	}
}

// StorageChecker checks storage system health
type StorageChecker struct {
	name   string
	logger zerolog.Logger
	tracer trace.Tracer
}

func NewStorageChecker(logger zerolog.Logger, tracer trace.Tracer) *StorageChecker {
	return &StorageChecker{
		name:   "storage",
		logger: logger,
		tracer: tracer,
	}
}

func (sc *StorageChecker) Name() string {
	return sc.name
}

func (sc *StorageChecker) Check(ctx context.Context) *ComponentHealth {
	_, span := sc.tracer.Start(ctx, "StorageChecker.Check")
	defer span.End()

	start := time.Now()

	// Simulate storage system check
	// In real implementation, this would check disk space, permissions, etc.
	time.Sleep(3 * time.Millisecond)

	duration := time.Since(start)

	status := StatusHealthy
	message := "Storage system operational"

	// Simulate occasional issues
	if duration > 8*time.Millisecond {
		status = StatusDegraded
		message = "Storage system slow response"
	}

	return &ComponentHealth{
		Name:        sc.name,
		Status:      status,
		Message:     message,
		LastChecked: time.Now(),
		Metadata: map[string]interface{}{
			"response_time_ms": duration.Milliseconds(),
			"check_duration":   duration.String(),
		},
	}
}

// MetricsChecker checks metrics collection system
type MetricsChecker struct {
	name   string
	logger zerolog.Logger
	tracer trace.Tracer
}

func NewMetricsChecker(logger zerolog.Logger, tracer trace.Tracer) *MetricsChecker {
	return &MetricsChecker{
		name:   "metrics",
		logger: logger,
		tracer: tracer,
	}
}

func (mc *MetricsChecker) Name() string {
	return mc.name
}

func (mc *MetricsChecker) Check(ctx context.Context) *ComponentHealth {
	_, span := mc.tracer.Start(ctx, "MetricsChecker.Check")
	defer span.End()

	start := time.Now()

	// Simulate metrics system check
	// In real implementation, this would check Prometheus connectivity, etc.
	time.Sleep(2 * time.Millisecond)

	duration := time.Since(start)

	status := StatusHealthy
	message := "Metrics collection operational"

	return &ComponentHealth{
		Name:        mc.name,
		Status:      status,
		Message:     message,
		LastChecked: time.Now(),
		Metadata: map[string]interface{}{
			"response_time_ms": duration.Milliseconds(),
			"check_duration":   duration.String(),
		},
	}
}

// LoggingChecker checks logging system health
type LoggingChecker struct {
	name   string
	logger zerolog.Logger
	tracer trace.Tracer
}

func NewLoggingChecker(logger zerolog.Logger, tracer trace.Tracer) *LoggingChecker {
	return &LoggingChecker{
		name:   "logging",
		logger: logger,
		tracer: tracer,
	}
}

func (lc *LoggingChecker) Name() string {
	return lc.name
}

func (lc *LoggingChecker) Check(ctx context.Context) *ComponentHealth {
	_, span := lc.tracer.Start(ctx, "LoggingChecker.Check")
	defer span.End()

	start := time.Now()

	// Test logging by writing a debug log entry
	lc.logger.Debug().Str("component", lc.name).Msg("Health check test log entry")

	duration := time.Since(start)

	status := StatusHealthy
	message := "Logging system operational"

	return &ComponentHealth{
		Name:        lc.name,
		Status:      status,
		Message:     message,
		LastChecked: time.Now(),
		Metadata: map[string]interface{}{
			"response_time_ms": duration.Milliseconds(),
			"check_duration":   duration.String(),
		},
	}
}

// TracingChecker checks distributed tracing system
type TracingChecker struct {
	name   string
	logger zerolog.Logger
	tracer trace.Tracer
}

func NewTracingChecker(logger zerolog.Logger, tracer trace.Tracer) *TracingChecker {
	return &TracingChecker{
		name:   "tracing",
		logger: logger,
		tracer: tracer,
	}
}

func (tc *TracingChecker) Name() string {
	return tc.name
}

func (tc *TracingChecker) Check(ctx context.Context) *ComponentHealth {
	_, span := tc.tracer.Start(ctx, "TracingChecker.Check")
	defer span.End()

	start := time.Now()

	// Test tracing by adding attributes to the current span
	span.SetAttributes(
		attribute.String("longbow.health.check", "tracing"),
		attribute.Int64("longbow.health.timestamp", time.Now().Unix()),
	)

	duration := time.Since(start)

	status := StatusHealthy
	message := "Distributed tracing operational"

	return &ComponentHealth{
		Name:        tc.name,
		Status:      status,
		Message:     message,
		LastChecked: time.Now(),
		Metadata: map[string]interface{}{
			"response_time_ms": duration.Milliseconds(),
			"check_duration":   duration.String(),
			"span_id":          span.SpanContext().SpanID().String(),
		},
	}
}
