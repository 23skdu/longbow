package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestDefaultLoggerConfig(t *testing.T) {
	cfg := DefaultLoggerConfig()
	if cfg.Level != InfoLevel {
		t.Errorf("expected Level=InfoLevel, got %v", cfg.Level)
	}
	if !cfg.EnableConsole {
		t.Error("expected EnableConsole=true")
	}
	if cfg.EnableFile {
		t.Error("expected EnableFile=false")
	}
	if cfg.EnableJSON {
		t.Error("expected EnableJSON=false")
	}
	if cfg.Component != "" {
		t.Errorf("expected Component=\"\", got %q", cfg.Component)
	}
}

func TestNewStructuredLogger(t *testing.T) {
	l := NewStructuredLogger(DefaultLoggerConfig())
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
	if l.level != InfoLevel {
		t.Errorf("expected level=InfoLevel, got %v", l.level)
	}
}

func TestNewStructuredLoggerJSON(t *testing.T) {
	cfg := DefaultLoggerConfig()
	cfg.EnableJSON = true
	l := NewStructuredLogger(cfg)
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestGetZerologLevel(t *testing.T) {
	tests := []struct {
		input LogLevel
		want zerolog.Level
	}{
		{DebugLevel, zerolog.DebugLevel},
		{InfoLevel, zerolog.InfoLevel},
		{WarnLevel, zerolog.WarnLevel},
		{ErrorLevel, zerolog.ErrorLevel},
		{FatalLevel, zerolog.FatalLevel},
		{LogLevel(99), zerolog.InfoLevel},
	}
	for _, tt := range tests {
		got := getZerologLevel(tt.input)
		if got != tt.want {
			t.Errorf("getZerologLevel(%v) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestStructuredLogger_Debug(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  DebugLevel,
	}
	ctx := context.Background()
	l.Debug(ctx, "debug message")
	if !strings.Contains(buf.String(), "debug message") {
		t.Errorf("expected debug message, got %q", buf.String())
	}
}

func TestStructuredLogger_Debug_LevelFiltered(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  ErrorLevel,
	}
	ctx := context.Background()
	l.Debug(ctx, "should not appear")
	if buf.Len() > 0 {
		t.Errorf("expected no output, got %q", buf.String())
	}
}

func TestStructuredLogger_Info(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  InfoLevel,
	}
	ctx := context.Background()
	l.Info(ctx, "info message")
	if !strings.Contains(buf.String(), "info message") {
		t.Errorf("expected info message, got %q", buf.String())
	}
}

func TestStructuredLogger_Info_WithFields(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  InfoLevel,
	}
	ctx := context.Background()
	l.Info(ctx, "with fields", map[string]any{"key": "val"})
	if !strings.Contains(buf.String(), "with fields") {
		t.Errorf("expected message, got %q", buf.String())
	}
	if !strings.Contains(buf.String(), "key") || !strings.Contains(buf.String(), "val") {
		t.Errorf("expected key=val in output, got %q", buf.String())
	}
}

func TestStructuredLogger_Info_WithContextFields(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  InfoLevel,
	}
	ctx := context.WithValue(context.Background(), "component", "test-comp")
	ctx = context.WithValue(ctx, "trace_id", "trace-123")
	ctx = context.WithValue(ctx, "span_id", "span-456")
	l.Info(ctx, "with ctx")
	output := buf.String()
	if !strings.Contains(output, "test-comp") {
		t.Errorf("expected component, got %q", output)
	}
	if !strings.Contains(output, "trace-123") {
		t.Errorf("expected trace_id, got %q", output)
	}
	if !strings.Contains(output, "span-456") {
		t.Errorf("expected span_id, got %q", output)
	}
}

func TestStructuredLogger_Warn(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  WarnLevel,
	}
	ctx := context.Background()
	l.Warn(ctx, "warn message")
	if !strings.Contains(buf.String(), "warn message") {
		t.Errorf("expected warn message, got %q", buf.String())
	}
}

func TestStructuredLogger_Error(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  ErrorLevel,
	}
	ctx := context.Background()
	l.Error(ctx, "error message")
	if !strings.Contains(buf.String(), "error message") {
		t.Errorf("expected error message, got %q", buf.String())
	}
}

func TestStructuredLogger_Error_LevelFiltered(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  FatalLevel,
	}
	ctx := context.Background()
	l.Error(ctx, "should not appear")
	if buf.Len() > 0 {
		t.Errorf("expected no output, got %q", buf.String())
	}
}

func TestStructuredLogger_WithComponent(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  InfoLevel,
	}
	sub := l.WithComponent("my-component")
	sub.Info(context.Background(), "component test")
	if !strings.Contains(buf.String(), "my-component") {
		t.Errorf("expected component in output, got %q", buf.String())
	}
}

func TestStructuredLogger_WithTrace(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  InfoLevel,
	}
	sub := l.WithTrace("tid-1", "sid-1")
	sub.Info(context.Background(), "trace test")
	output := buf.String()
	if !strings.Contains(output, "tid-1") {
		t.Errorf("expected trace_id in output, got %q", output)
	}
	if !strings.Contains(output, "sid-1") {
		t.Errorf("expected span_id in output, got %q", output)
	}
}

func TestStructuredLogger_WithFields(t *testing.T) {
	var buf bytes.Buffer
	l := &StructuredLogger{
		logger: zerolog.New(&buf),
		level:  InfoLevel,
	}
	sub := l.WithFields(map[string]any{"env": "test"})
	sub.Info(context.Background(), "fields test")
	if !strings.Contains(buf.String(), "test") {
		t.Errorf("expected field in output, got %q", buf.String())
	}
}

func TestGetFields(t *testing.T) {
	result := getFields(
		map[string]any{"a": 1, "b": 2},
		map[string]any{"c": 3},
	)
	if len(result) != 3 {
		t.Errorf("expected 3 fields, got %d", len(result))
	}
	if result["a"] != 1 {
		t.Errorf("expected a=1, got %v", result["a"])
	}
}

func TestGetFields_Overwrite(t *testing.T) {
	result := getFields(
		map[string]any{"key": "first"},
		map[string]any{"key": "second"},
	)
	if result["key"] != "second" {
		t.Errorf("expected key=second, got %v", result["key"])
	}
}

func TestGetFields_Nil(t *testing.T) {
	result := getFields()
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestGlobalLogger_InitAndLog(t *testing.T) {
	globalLogger = nil
	cfg := DefaultLoggerConfig()
	InitGlobalLogger(cfg)
	if globalLogger == nil {
		t.Fatal("expected global logger to be initialized")
	}

	ctx := context.Background()
	Debug(ctx, "debug from global")
	Info(ctx, "info from global")
	Warn(ctx, "warn from global")
	Error(ctx, "error from global")
}

func TestGlobalLogger_WithoutInit(t *testing.T) {
	globalLogger = nil
	ctx := context.Background()
	Debug(ctx, "no-op")
	Info(ctx, "no-op")
	Warn(ctx, "no-op")
	Error(ctx, "no-op")
}

func TestGlobalLogger_WithComponent(t *testing.T) {
	globalLogger = nil
	sub := WithComponent("test-comp")
	if sub.StructuredLogger != nil {
		t.Error("expected nil StructuredLogger when global not initialized")
	}

	InitGlobalLogger(DefaultLoggerConfig())
	sub = WithComponent("test-comp")
	if sub.StructuredLogger == nil {
		t.Error("expected non-nil StructuredLogger")
	}
}

func TestGlobalLogger_WithTrace(t *testing.T) {
	globalLogger = nil
	sub := WithTrace("tid", "sid")
	if sub.StructuredLogger != nil {
		t.Error("expected nil StructuredLogger when global not initialized")
	}
}

func TestGlobalLogger_WithFields(t *testing.T) {
	globalLogger = nil
	sub := WithFields(map[string]any{"k": "v"})
	if sub.StructuredLogger != nil {
		t.Error("expected nil StructuredLogger when global not initialized")
	}
}

func TestMetricsHook_NoLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	hook := MetricsHook{}
	evt := logger.Info()
	hook.Run(evt, zerolog.NoLevel, "test")
	// Should not panic; NoLevel is skipped
}

func TestMetricsHook_ErrorLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	hook := MetricsHook{}
	evt := logger.Error()
	hook.Run(evt, zerolog.ErrorLevel, "error msg")
	// Should not panic
}

func TestConsoleFormat(t *testing.T) {
	var buf bytes.Buffer
	logger, err := NewLogger(Config{
		Format: "console",
		Level:  "info",
		Output: &buf,
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}
	logger.Info().Msg("console test")
	if buf.Len() == 0 {
		t.Error("expected console output")
	}
}

func TestInvalidFormatDefaultsToJSON(t *testing.T) {
	var buf bytes.Buffer
	logger, err := NewLogger(Config{
		Format: "unknown",
		Level:  "info",
		Output: &buf,
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}
	logger.Info().Msg("default json test")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("expected valid JSON, got error: %v, output: %s", err, buf.String())
	}
}

func TestNilOutputDefaultsToStdout(t *testing.T) {
	// Should not panic when Output is nil (defaults to os.Stdout)
	logger, err := NewLogger(Config{
		Format: "json",
		Level:  "info",
		Output: nil,
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}
	logger.Info().Msg("nil output test")
}
