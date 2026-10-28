package logging

import (
	"context"
	"os"

	"github.com/rs/zerolog"
)

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

type StructuredLogger struct {
	logger zerolog.Logger
	level  LogLevel
}

type LoggerConfig struct {
	Level         LogLevel
	EnableConsole bool
	EnableFile    bool
	FilePath      string
	EnableJSON    bool
	EnableTrace   bool
	Component     string
}

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

type ContextKey string

const (
	ComponentKey ContextKey = "component"
	TraceIDKey   ContextKey = "trace_id"
	SpanIDKey    ContextKey = "span_id"
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

func (l *StructuredLogger) WithComponent(component string) *StructuredLogger {
	return &StructuredLogger{
		logger: l.logger.With().Str("component", component).Logger(),
		level:  l.level,
	}
}

func (l *StructuredLogger) WithTrace(traceID, spanID string) *StructuredLogger {
	return &StructuredLogger{
		logger: l.logger.With().Str("trace_id", traceID).Str("span_id", spanID).Logger(),
		level:  l.level,
	}
}

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

type GlobalLogger struct {
	*StructuredLogger
}

var globalLogger *GlobalLogger

func InitGlobalLogger(config LoggerConfig) {
	if globalLogger == nil {
		globalLogger = &GlobalLogger{
			StructuredLogger: NewStructuredLogger(config),
		}
	}
}

func Debug(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Debug(ctx, msg, fields...)
	}
}

func Info(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Info(ctx, msg, fields...)
	}
}

func Warn(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Warn(ctx, msg, fields...)
	}
}

func Error(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Error(ctx, msg, fields...)
	}
}

func Fatal(ctx context.Context, msg string, fields ...map[string]any) {
	if globalLogger != nil {
		globalLogger.Fatal(ctx, msg, fields...)
	}
}

func WithComponent(component string) *GlobalLogger {
	if globalLogger != nil {
		return &GlobalLogger{
			StructuredLogger: globalLogger.WithComponent(component),
		}
	}
	return &GlobalLogger{}
}

func WithTrace(traceID, spanID string) *GlobalLogger {
	if globalLogger != nil {
		return &GlobalLogger{
			StructuredLogger: globalLogger.WithTrace(traceID, spanID),
		}
	}
	return &GlobalLogger{}
}

func WithFields(fields map[string]any) *GlobalLogger {
	if globalLogger != nil {
		return &GlobalLogger{
			StructuredLogger: globalLogger.WithFields(fields),
		}
	}
	return &GlobalLogger{}
}
