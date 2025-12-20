package logging

import (
	"fmt"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Prometheus metrics for logging operations
var (
	// LogEntriesTotal counts log entries by level
	LogEntriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_log_entries_total",
			Help: "Total number of log entries by level",
		},
		[]string{"level"},
	)

	// LogErrorsTotal counts error-level log entries specifically
	LogErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_log_errors_total",
			Help: "Total number of error log entries",
		},
	)
)

// Config holds logger configuration options
type Config struct {
	// Format specifies the log output format: "json" or "text"
	Format string
	// Level specifies the minimum log level: "debug", "info", "warn", "error"
	Level string
	// Output specifies where logs are written (defaults to os.Stdout)
	Output zapcore.WriteSyncer
}

// DefaultConfig returns the default logger configuration
func DefaultConfig() Config {
	return Config{
		Format: "json",
		Level:  "info",
		Output: os.Stdout,
	}
}

// NewLogger creates a new zap logger based on the provided configuration
func NewLogger(cfg Config) (*zap.Logger, error) {
	// Parse log level
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	// Set default output
	output := cfg.Output
	if output == nil {
		output = os.Stdout
	}

	// Create encoder config based on format
	var encoder zapcore.Encoder
	switch strings.ToLower(cfg.Format) {
	case "json":
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	case "text", "console":
		encoderConfig := zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	default:
		// Default to JSON if not specified
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	// Create core with metrics hook
	core := zapcore.NewCore(encoder, output, level)

	// Wrap core with metrics hook
	metricsCore := &metricsHookCore{Core: core}

	// Build logger with caller info
	logger := zap.New(metricsCore, zap.AddCaller(), zap.AddCallerSkip(0))

	return logger, nil
}

// DiscardLogger returns a logger that discards all output (useful for tests)
func DiscardLogger() *zap.Logger {
	return zap.NewNop()
}

// parseLevel converts a string level to zapcore.Level
func parseLevel(level string) (zapcore.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "dpanic":
		return zapcore.DPanicLevel, nil
	case "panic":
		return zapcore.PanicLevel, nil
	case "fatal":
		return zapcore.FatalLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("invalid log level: %s", level)
	}
}

// metricsHookCore wraps a zapcore.Core to add Prometheus metrics
type metricsHookCore struct {
	zapcore.Core
}

// Check determines whether the entry should be logged
//
//nolint:gocritic // hugeParam: interface requires value receiver
func (c *metricsHookCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

// Write logs the entry and increments Prometheus metrics
//
//nolint:gocritic // hugeParam: interface requires value receiver
func (c *metricsHookCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	// Increment level-specific counter
	LogEntriesTotal.WithLabelValues(entry.Level.String()).Inc()

	// Special counter for errors
	if entry.Level >= zapcore.ErrorLevel {
		LogErrorsTotal.Inc()
	}

	return c.Core.Write(entry, fields)
}

// With creates a child core with additional fields
func (c *metricsHookCore) With(fields []zapcore.Field) zapcore.Core {
	return &metricsHookCore{Core: c.Core.With(fields)}
}
