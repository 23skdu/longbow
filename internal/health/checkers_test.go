package health

import (
	"context"
	"testing"
	"time"

	zerolog "github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

func TestComponentHealth_JSON(t *testing.T) {
	h := ComponentHealth{
		Name:        "test",
		Status:      StatusHealthy,
		Message:     "ok",
		LastChecked: time.Now(),
		Metadata:    map[string]interface{}{"key": "value"},
	}
	
	if h.Name != "test" {
		t.Errorf("expected name test, got %s", h.Name)
	}
	if h.Status != StatusHealthy {
		t.Errorf("expected status healthy, got %s", h.Status)
	}
}

func TestHealthStatus_String(t *testing.T) {
	tests := []struct {
		status   HealthStatus
		expected string
	}{
		{StatusHealthy, "healthy"},
		{StatusDegraded, "degraded"},
		{StatusUnhealthy, "unhealthy"},
	}
	
	for _, tc := range tests {
		if string(tc.status) != tc.expected {
			t.Errorf("expected %s, got %s", tc.expected, tc.status)
		}
	}
}

func TestNewDatabaseChecker(t *testing.T) {
	logger := zerolog.New(nil)
	tracer := trace.NewNoopTracerProvider().Tracer("test")
	
	checker := NewDatabaseChecker(&logger, tracer)
	if checker == nil {
		t.Error("expected non-nil checker")
	}
	if checker.Name() != "database" {
		t.Errorf("expected database, got %s", checker.Name())
	}
	
	ctx := context.Background()
	health := checker.Check(ctx)
	if health == nil {
		t.Error("expected non-nil health")
	}
	if health.Status == "" {
		t.Error("expected status to be set")
	}
}

func TestNewStorageChecker(t *testing.T) {
	logger := zerolog.New(nil)
	tracer := trace.NewNoopTracerProvider().Tracer("test")
	
	checker := NewStorageChecker(logger, tracer)
	if checker == nil {
		t.Error("expected non-nil checker")
	}
	if checker.Name() != "storage" {
		t.Errorf("expected storage, got %s", checker.Name())
	}
	
	ctx := context.Background()
	health := checker.Check(ctx)
	if health == nil {
		t.Error("expected non-nil health")
	}
}

func TestNewMetricsChecker(t *testing.T) {
	logger := zerolog.New(nil)
	tracer := trace.NewNoopTracerProvider().Tracer("test")
	
	checker := NewMetricsChecker(logger, tracer)
	if checker == nil {
		t.Error("expected non-nil checker")
	}
	if checker.Name() != "metrics" {
		t.Errorf("expected metrics, got %s", checker.Name())
	}
	
	ctx := context.Background()
	health := checker.Check(ctx)
	if health == nil {
		t.Error("expected non-nil health")
	}
}

func TestNewLoggingChecker(t *testing.T) {
	logger := zerolog.New(nil)
	tracer := trace.NewNoopTracerProvider().Tracer("test")
	
	checker := NewLoggingChecker(logger, tracer)
	if checker == nil {
		t.Error("expected non-nil checker")
	}
	if checker.Name() != "logging" {
		t.Errorf("expected logging, got %s", checker.Name())
	}
	
	ctx := context.Background()
	health := checker.Check(ctx)
	if health == nil {
		t.Error("expected non-nil health")
	}
}
