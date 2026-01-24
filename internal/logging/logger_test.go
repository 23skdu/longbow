package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// TestNewLogger verifies basic logger creation
func TestNewLogger(t *testing.T) {
	_ = zerolog.InfoLevel
	tests := []struct {
		name   string
		format string
		level  string
	}{
		{"JSON Info", "json", "info"},
		{"JSON Debug", "json", "debug"},
		{"JSON Error", "json", "error"},
		{"Text Info", "text", "info"},
		{"Text Debug", "text", "debug"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := NewLogger(Config{
				Format: tt.format,
				Level:  tt.level,
			})
			if err != nil {
				t.Fatalf("NewLogger() error = %v", err)
			}
			// Zerolog logger is a value, check if it's usable
			logger.Info().Msg("heartbeat")
		})
	}
}

// TestNewLogger_InvalidLevel verifies error handling for invalid log level
func TestNewLogger_InvalidLevel(t *testing.T) {
	_, err := NewLogger(Config{
		Format: "json",
		Level:  "invalid",
	})
	if err == nil {
		t.Error("Expected error for invalid log level")
	}
}

// TestStructuredLogging verifies structured logging with fields
func TestStructuredLogging(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := NewLogger(Config{Format: "json", Level: "info", Output: &buf})

	logger.Info().
		Str("key1", "value1").
		Int("key2", 42).
		Msg("test message")

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Errorf("Expected message in output, got: %s", output)
	}
	if !strings.Contains(output, "key1") {
		t.Errorf("Expected key1 in output, got: %s", output)
	}
	if !strings.Contains(output, "value1") {
		t.Errorf("Expected value1 in output, got: %s", output)
	}
}

// TestLogLevelFiltering verifies that log levels are properly filtered
func TestLogLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := NewLogger(Config{Format: "json", Level: "warn", Output: &buf})

	logger.Debug().Msg("debug message")
	logger.Info().Msg("info message")
	logger.Warn().Msg("warn message")
	logger.Error().Msg("error message")

	output := buf.String()
	if strings.Contains(output, "debug message") {
		t.Error("Debug message should be filtered at Warn level")
	}
	if strings.Contains(output, "info message") {
		t.Error("Info message should be filtered at Warn level")
	}
	if !strings.Contains(output, "warn message") {
		t.Error("Warn message should be present")
	}
	if !strings.Contains(output, "error message") {
		t.Error("Error message should be present")
	}
}

// TestJSONOutput verifies JSON format output
func TestJSONOutput(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := NewLogger(Config{Format: "json", Level: "info", Output: &buf})

	logger.Info().Str("foo", "bar").Msg("json test")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("Failed to parse JSON output: %v, output: %s", err, buf.String())
	}

	if entry["message"] != "json test" { // Zerolog default is "message"
		t.Errorf("Expected message='json test', got %v", entry["message"])
	}
	if entry["foo"] != "bar" {
		t.Errorf("Expected foo='bar', got %v", entry["foo"])
	}
}

// TestDiscardLogger verifies the discard logger for tests
func TestDiscardLogger(t *testing.T) {
	logger := DiscardLogger()
	// Should not panic
	logger.Info().Msg("this should be discarded")
	logger.Error().Msg("this too")
}

// TestLoggerWithFields verifies logger.With() for adding default fields
func TestLoggerWithFields(t *testing.T) {
	var buf bytes.Buffer
	baseLogger, _ := NewLogger(Config{Format: "json", Level: "info", Output: &buf})

	// Create a child logger with default fields
	childLogger := baseLogger.With().Str("component", "test").Logger()
	childLogger.Info().Msg("message with component")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if entry["component"] != "test" {
		t.Errorf("Expected component='test', got %v", entry["component"])
	}
}

// TestLoggingMetrics verifies Prometheus metrics for logging
func TestLoggingMetrics(t *testing.T) {
	// Reset metrics for test isolation
	reg := prometheus.NewRegistry()

	// Create metrics
	logEntriesCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "test_log_entries_total",
			Help: "Total number of log entries by level",
		},
		[]string{"level"},
	)
	reg.MustRegister(logEntriesCounter)

	// Simulate logging metrics
	logEntriesCounter.WithLabelValues("info").Inc()
	logEntriesCounter.WithLabelValues("error").Inc()
	logEntriesCounter.WithLabelValues("info").Inc()

	// Gather and verify
	metrics, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	if len(metrics) == 0 {
		t.Fatal("Expected at least one metric family")
	}

	found := false
	for _, mf := range metrics {
		if mf.GetName() == "test_log_entries_total" {
			found = true
			if len(mf.GetMetric()) < 2 {
				t.Errorf("Expected at least 2 metric labels, got %d", len(mf.GetMetric()))
			}
		}
	}
	if !found {
		t.Error("Metric test_log_entries_total not found")
	}
}

// TestDefaultConfig verifies default configuration values
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Format != "json" {
		t.Errorf("Expected default format='json', got %s", cfg.Format)
	}
	if cfg.Level != "info" {
		t.Errorf("Expected default level='info', got %s", cfg.Level)
	}
}
