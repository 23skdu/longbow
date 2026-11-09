package logging

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/rs/zerolog"
)

// fuzzLevelAccepted mirrors zerolog.ParseLevel exactly as NewLogger applies it
// (to the lower-cased level string): one of the named levels, or an integer
// in [-128, 127].
func fuzzLevelAccepted(level string) bool {
	l := strings.ToLower(level)
	named := []string{
		zerolog.LevelFieldMarshalFunc(zerolog.TraceLevel),
		zerolog.LevelFieldMarshalFunc(zerolog.DebugLevel),
		zerolog.LevelFieldMarshalFunc(zerolog.InfoLevel),
		zerolog.LevelFieldMarshalFunc(zerolog.WarnLevel),
		zerolog.LevelFieldMarshalFunc(zerolog.ErrorLevel),
		zerolog.LevelFieldMarshalFunc(zerolog.FatalLevel),
		zerolog.LevelFieldMarshalFunc(zerolog.PanicLevel),
		zerolog.LevelFieldMarshalFunc(zerolog.Disabled),
		zerolog.LevelFieldMarshalFunc(zerolog.NoLevel),
	}
	for _, n := range named {
		if strings.EqualFold(l, n) {
			return true
		}
	}
	if i, err := strconv.Atoi(l); err == nil && i >= -128 && i <= 127 {
		return true
	}
	return false
}

// FuzzNewLogger checks the logger configuration contract: invalid levels are
// rejected with an error, valid levels produce a logger, and every emitted
// event is a single well-formed JSON line that round-trips its message and
// fields (no newline or control-character injection).
func FuzzNewLogger(f *testing.F) {
	f.Add("json", "info", "hello world", "value")
	f.Add("json", "bogus-level", "x", "y")           // invalid level string
	f.Add("json", "128", "x", "y")                   // out-of-range numeric level
	f.Add("text", "debug", "console line", "v")      // console format branch
	f.Add("console", "warn", "", "")                 // empty message is omitted
	f.Add("JSON", "", "line\nbreak", "field\nbreak") // newline injection attempt
	f.Add("json", "5", "numeric level", "v")         // numeric level -> panic level
	f.Add("json", "trace", "globally filtered", "v") // suppressed by global level

	f.Fuzz(func(t *testing.T, format, level, msg, field string) {
		if len(format) > 64 || len(level) > 64 || len(msg) > 4096 || len(field) > 4096 {
			return
		}

		var buf bytes.Buffer
		logger, err := NewLogger(Config{Format: format, Level: level, Output: &buf})
		if err != nil {
			if fuzzLevelAccepted(level) {
				t.Fatalf("NewLogger rejected valid level %q", level)
			}
			return
		}
		if !fuzzLevelAccepted(level) {
			t.Fatalf("NewLogger accepted invalid level %q", level)
		}

		lvl, lerr := zerolog.ParseLevel(strings.ToLower(level))
		if lerr != nil {
			t.Fatalf("ParseLevel(%q): %v", level, lerr)
		}

		// Emit at exactly the configured level: the event is suppressed only
		// when zerolog itself filters it (the disabled level, or a level
		// below the global minimum).
		wantEvent := lvl != zerolog.Disabled && lvl >= zerolog.GlobalLevel()
		ev := logger.WithLevel(lvl)
		if !wantEvent {
			if ev != nil {
				t.Errorf("WithLevel(%v) = non-nil, want nil for a filtered level", lvl)
			}
			if buf.Len() != 0 {
				t.Errorf("filtered level %q wrote %d bytes", level, buf.Len())
			}
			return
		}
		if ev == nil {
			t.Fatalf("WithLevel(%v) was filtered, want an enabled event", lvl)
		}
		ev.Str("k", field).Msg(msg)

		if buf.Len() == 0 {
			t.Fatalf("event for level %q produced no output", level)
		}

		raw := buf.String()
		if strings.ToLower(format) == "text" || strings.ToLower(format) == "console" {
			// Console output is human-formatted; require that it was
			// produced, but make no claim about its shape.
			return
		}

		// JSON output: exactly one line, valid JSON, and the message and
		// fields survive verbatim (control characters must be escaped, so
		// no injected newline can split the record).
		if !strings.HasSuffix(raw, "\n") || strings.Count(raw, "\n") != 1 {
			t.Fatalf("event must be exactly one line, got %q", raw)
		}
		var decoded map[string]string
		if err := json.Unmarshal([]byte(strings.TrimSuffix(raw, "\n")), &decoded); err != nil {
			t.Fatalf("event is not valid JSON: %v (%q)", err, raw)
		}

		if lvl != zerolog.NoLevel {
			if got, want := decoded["level"], zerolog.LevelFieldMarshalFunc(lvl); got != want {
				t.Errorf("level field = %q, want %q", got, want)
			}
		}
		// Round-trip comparisons are only defined for valid UTF-8; zerolog
		// replaces invalid sequences with U+FFFD in the encoded output.
		if utf8.ValidString(msg) {
			if msg == "" {
				if _, ok := decoded["message"]; ok {
					t.Errorf("empty message must be omitted, got %q", raw)
				}
			} else if got := decoded["message"]; got != msg {
				t.Errorf("message = %q, want %q", got, msg)
			}
		}
		if utf8.ValidString(field) {
			if got := decoded["k"]; got != field {
				t.Errorf("field = %q, want %q", got, field)
			}
		}
	})
}
