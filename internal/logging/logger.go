package logging

import (

	"io"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
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
	Output io.Writer
}

// DefaultConfig returns the default logger configuration
func DefaultConfig() Config {
	return Config{
		Format: "json",
		Level:  "info",
		Output: os.Stdout,
	}
}

// NewLogger creates a new zerolog logger based on the provided configuration
func NewLogger(cfg Config) (zerolog.Logger, error) {
	// Parse log level
	level, err := zerolog.ParseLevel(strings.ToLower(cfg.Level))
	if err != nil {
		return zerolog.Logger{}, err
	}

	// Set default output
	output := cfg.Output
	if output == nil {
		output = os.Stdout
	}

	// Create writer based on format
	var writer io.Writer
	switch strings.ToLower(cfg.Format) {
	case "json":
		writer = output
	case "text", "console":
		writer = zerolog.ConsoleWriter{Out: output, TimeFormat: "2006-01-02 15:04:05"}
	default:
		// Default to JSON
		writer = output
	}

	// Build logger with metrics hook
	logger := zerolog.New(writer).
		Level(level).
		With().
		Timestamp().
		Caller().
		Logger().
		Hook(MetricsHook{})

	return logger, nil
}

// DiscardLogger returns a logger that discards all output (useful for tests)
func DiscardLogger() zerolog.Logger {
	return zerolog.Nop()
}

// MetricsHook implements zerolog.Hook to increment Prometheus metrics
type MetricsHook struct{}

// Run increments Prometheus metrics based on the log level
func (h MetricsHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level == zerolog.NoLevel {
		return
	}

	levelStr := level.String()
	LogEntriesTotal.WithLabelValues(levelStr).Inc()

	if level >= zerolog.ErrorLevel {
		LogErrorsTotal.Inc()
	}
}

