package security

import (
	"strings"
	"testing"
)

// FuzzValidateInput checks ValidateInput against its documented rules:
// inputs longer than 1024 bytes, containing HTML/quote/NUL characters,
// SQL keywords (case-insensitive) or path-traversal patterns must be
// rejected; everything else must be accepted. Arbitrary (including invalid
// UTF-8) input must never panic.
func FuzzValidateInput(f *testing.F) {
	f.Add("valid-user-input")
	f.Add("")
	f.Add("SELECT * FROM users")
	f.Add("<script>alert(1)</script>")
	f.Add("../../etc/passwd")
	f.Add("user@domain.com")
	f.Add("has\\backslash")
	f.Add(strings.Repeat("a", 1030)) // exceeds the 1024-byte limit

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 2048 {
			return
		}

		wantErr := len(input) > 1024
		if !wantErr {
			for _, ch := range []string{"<", ">", "&", "\"", "'", "\x00"} {
				if strings.Contains(input, ch) {
					wantErr = true
					break
				}
			}
		}
		if !wantErr {
			lower := strings.ToLower(input)
			for _, kw := range []string{"select", "insert", "delete", "drop"} {
				if strings.Contains(lower, kw) {
					wantErr = true
					break
				}
			}
		}
		if !wantErr && (strings.Contains(input, "..") || strings.Contains(input, "\\")) {
			wantErr = true
		}

		err := ValidateInput(input)
		if wantErr && err == nil {
			t.Fatalf("ValidateInput(%q) = nil, want error", input)
		}
		if !wantErr && err != nil {
			t.Fatalf("ValidateInput(%q) = %v, want nil", input, err)
		}
	})
}

// FuzzSanitizeStringStripsMarkup verifies the sanitizer's core guarantees on
// arbitrary (including invalid UTF-8) input: the result never contains an
// angle bracket, is whitespace-trimmed, and is deterministic.
func FuzzSanitizeStringStripsMarkup(f *testing.F) {
	f.Add("<b>hello</b>")
	f.Add("javascript:alert(1)")
	f.Add("plain text")
	f.Add("  <a href='data:text/html'>x</a>  ")
	f.Add("\x00<>data:")
	f.Add("javajavascript:script:")

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			return
		}

		s := NewInputSanitizer()
		out := s.SanitizeString(input)

		if strings.ContainsAny(out, "<>") {
			t.Fatalf("SanitizeString(%q) left an angle bracket: %q", input, out)
		}
		if trimmed := strings.TrimSpace(out); trimmed != out {
			t.Fatalf("SanitizeString(%q) result not trimmed: %q", input, out)
		}
		if again := s.SanitizeString(input); again != out {
			t.Fatalf("SanitizeString not deterministic: %q vs %q", out, again)
		}
	})
}
