package logging

import (
	"context"
	"os"

	"github.com/rs/zerolog"
)

// LogLevel represents a log level value.
type LogLevel int

const (
	// DebugLevel is a debug level constant.
	DebugLevel LogLevel = iota
	// InfoLevel is a info level constant.
	InfoLevel
	// WarnLevel is a warn level constant.
	WarnLevel
	// ErrorLevel is a error level constant.
	ErrorLevel
	// FatalLevel is a fatal level constant.
	FatalLevel
)

// StructuredLogger represents a structured logger value.
type StructuredLogger struct {
	logger zerolog.Logger
	level  LogLevel
}

// LoggerConfig represents a logger config value.
type LoggerConfig struct {
	Level         LogLevel
	EnableConsole bool
	EnableFile    bool
	FilePath      string
	EnableJSON    bool
	EnableTrace   bool
	Component     string
}

// DefaultLoggerConfig returns the default LoggerConfig.
func DefaultLoggerConfig() LoggerConfig {
	return LoggerConfig{
		Level:         InfoLevel,
		EnableConsole: true,
		EnableFile:    false,
		EnableJSON:    false,
		EnableTrace:   false,
		Component:     "",
	}
}

// NewStructuredLogger constructs a new StructuredLogger.
func NewStructuredLogger(config LoggerConfig) *StructuredLogger {
	logger := zerolog.New(os.Stdout)
	if config.EnableJSON {
		logger = logger.With().Timestamp().Logger()
	}

	zerologLevel := getZerologLevel(config.Level)
	logger = logger.Level(zerologLevel)

	return &StructuredLogger{
		logger: logger,
		level:  config.Level,
	}
}

func getZerologLevel(level LogLevel) zerolog.Level {
	switch level {
	case DebugLevel:
		return zerolog.DebugLevel
	case InfoLevel:
		return zerolog.InfoLevel
	case WarnLevel:
		return zerolog.WarnLevel
	case ErrorLevel:
		return zerolog.ErrorLevel
	case FatalLevel:
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}

// ContextKey represents a context key value.
type ContextKey string

const (
	// ComponentKey is a component key constant.
	ComponentKey ContextKey = "component"
	// TraceIDKey is a trace id key constant.
	TraceIDKey ContextKey = "trace_id"
	// SpanIDKey is a span id key constant.
	SpanIDKey ContextKey = "span_id"
)

func extractCtxString(ctx context.Context, k ContextKey) string {
	if ctx == nil {
		return ""
	}
	if val := ctx.Value(k); val != nil {
		if str, ok := val.(string); ok {
			return str
		}
	}
	if val := ctx.Value(string(k)); val != nil {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (l *StructuredLogger) applyContext(event *zerolog.Event, ctx context.Context) *zerolog.Event {
	if comp := extractCtxString(ctx, ComponentKey); comp != "" {
		event = event.Str("component", comp)
	}
	if traceID := extractCtxString(ctx, TraceIDKey); traceID != "" {
		event = event.Str("trace_id", traceID)
	}
	if spanID := extractCtxString(ctx, SpanIDKey); spanID != "" {
		event = event.Str("span_id", spanID)
	}
	return event
}

// Debug runs the debug operation.
func (l *StructuredLogger) Debug(ctx context.Context, msg string, fields ...map[string]any) {
	if l.level <= DebugLevel {
		event := l.logger.Debug()
		if len(fields) > 0 {
			event = event.Fields(getFields(fields...))
		}
		event = l.applyContext(event, ctx)
		event.Msg(msg)
	}
}

// Info runs the info operation.
func (l *StructuredLogger) Info(ctx context.Context, msg string, fields ...map[string]any) {
	if l.level <= InfoLevel {
		event := l.logger.Info()
		if len(fields) > 0 {
			event = event.Fields(getFields(fields...))
		}
		event = l.applyContext(event, ctx)
		event.Msg(msg)
	}
}

// Warn runs the warn operation.
func (l *StructuredLogger) Warn(ctx context.Context, msg string, fields ...map[string]any) {
	if l.level <= WarnLevel {
		event := l.logger.Warn()
		if len(fields) > 0 {
			event = event.Fields(getFields(fields...))
		}
		event = l.applyContext(event, ctx)
		event.Msg(msg)
	}
}

// Error runs the error operation.
func (l *StructuredLogger) Error(ctx context.Context, msg string, fields ...map[string]any) {
	if l.level <= ErrorLevel {
		event := l.logger.Error()
		if len(fields) > 0 {
			event = event.Fields(getFields(fields...))
		}
		event = l.applyContext(event, ctx)
		event.Msg(msg)
	}
}

// Fatal runs the fatal operation.
func (l *StructuredLogger) Fatal(ctx context.Context, msg string, fields ...map[string]any) {
	if l.level <= FatalLevel {
		event := l.logger.Fatal()
		if len(fields) > 0 {
			event = event.Fields(getFields(fields...))
		}
		event = l.applyContext(event, ctx)
		event.Msg(msg)
		os.Exit(1)
	}
}

// WithComponent runs the with component operation.
func (l *StructuredLogger) WithComponent(component string) *StructuredLogger {
	return &StructuredLogger{
		logger: l.logger.With().Str("component", component).Logger(),
		level:  l.level,
	}
}

// WithTrace runs the with trace operation.
func (l *StructuredLogger) WithTrace(traceID, spanID string) *StructuredLogger {
	return &StructuredLogger{
		logger: l.logger.With().Str("trace_id", traceID).Str("span_id", spanID).Logger(),
		level:  l.level,
	}
}

// WithFields runs the with fields operation.
func (l *StructuredLogger) WithFields(fields map[string]any) *StructuredLogger {
	return &StructuredLogger{
		logger: l.logger.With().Fields(fields).Logger(),
		level:  l.level,
	}
}

func getFields(fields ...map[string]any) map[string]any {
	result := make(map[string]any)
	for _, fieldMap := range fields {
		for k, v := range fieldMap {
			result[k] = v
		}
	}
	return result
}

// GlobalLogger represents a global logger value.
type GlobalLogger struct {
	*StructuredLogger
}

var globalLogger *GlobalLogger

// InitGlobalLogger runs the init global logger operation.
func InitGlobalLogger(config LoggerConfig) {
	if globalLogger == nil {
		globalLogger = &GlobalLogger{
			StructuredLogger: NewStructuredLogger(config),
		}
	}
}

// Debug runs the debug operation.
func Debug(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Debug(ctx, msg, fields...)
	}
}

// Info runs the info operation.
func Info(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Info(ctx, msg, fields...)
	}
}

// Warn runs the warn operation.
func Warn(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Warn(ctx, msg, fields...)
	}
}

// Error runs the error operation.
func Error(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Error(ctx, msg, fields...)
	}
}

// Fatal runs the fatal operation.
func Fatal(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Fatal(ctx, msg, fields...)
	}
}

// WithComponent runs the with component operation.
func WithComponent(component string) *GlobalLogger {
	if globalLogger != nil {
		return &GlobalLogger{
			StructuredLogger: globalLogger.WithComponent(component),
		}
	}
	return &GlobalLogger{}
}

// WithTrace runs the with trace operation.
func WithTrace(traceID, spanID string) *GlobalLogger {
	if globalLogger != nil {
		return &GlobalLogger{
			StructuredLogger: globalLogger.WithTrace(traceID, spanID),
		}
	}
	return &GlobalLogger{}
}

// WithFields runs the with fields operation.
func WithFields(fields map[string]any) *GlobalLogger {
	if globalLogger != nil {
		return &GlobalLogger{
			StructuredLogger: globalLogger.WithFields(fields),
		}
	}
	return &GlobalLogger{}
}
