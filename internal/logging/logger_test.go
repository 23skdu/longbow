package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestNewLogger verifies basic logger creation
func TestNewLogger(t *testing.T) {
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
			if logger == nil {
				t.Fatal("NewLogger() returned nil logger")
			}
			_ = logger.Sync()
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
	logger := newTestLogger(&buf, zapcore.InfoLevel)

	logger.Info("test message",
		zap.String("key1", "value1"),
		zap.Int("key2", 42),
	)
	_ = logger.Sync()

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
	logger := newTestLogger(&buf, zapcore.WarnLevel)

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")
	_ = logger.Sync()

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
	logger := newTestJSONLogger(&buf, zapcore.InfoLevel)

	logger.Info("json test", zap.String("foo", "bar"))
	_ = logger.Sync()

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("Failed to parse JSON output: %v, output: %s", err, buf.String())
	}

	if entry["msg"] != "json test" {
		t.Errorf("Expected msg='json test', got %v", entry["msg"])
	}
	if entry["foo"] != "bar" {
		t.Errorf("Expected foo='bar', got %v", entry["foo"])
	}
}

// TestDiscardLogger verifies the discard logger for tests
func TestDiscardLogger(t *testing.T) {
	logger := DiscardLogger()
	if logger == nil {
		t.Fatal("DiscardLogger() returned nil")
	}
	// Should not panic
	logger.Info("this should be discarded")
	logger.Error("this too")
	_ = logger.Sync()
}

// TestLoggerWithFields verifies logger.With() for adding default fields
func TestLoggerWithFields(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := newTestJSONLogger(&buf, zapcore.InfoLevel)

	// Create a child logger with default fields
	childLogger := baseLogger.With(zap.String("component", "test"))
	childLogger.Info("message with component")
	_ = childLogger.Sync()

	var entry map[string]interface{}
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

// Helper: create test logger writing to buffer (console format)
func newTestLogger(buf *bytes.Buffer, level zapcore.Level) *zap.Logger {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(buf),
		level,
	)
	return zap.New(core)
}

// Helper: create test logger writing to buffer (JSON format)
func newTestJSONLogger(buf *bytes.Buffer, level zapcore.Level) *zap.Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "" // Disable timestamp for easier testing
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(buf),
		level,
	)
	return zap.New(core)
}
