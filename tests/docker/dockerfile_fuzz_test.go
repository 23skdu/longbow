package docker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// refDockerfileLines is an independent reference implementation of the line
// splitting contract: split on '\n', drop one trailing empty segment (i.e. a
// final newline), and strip a single trailing '\r' from every line — the
// bufio.ScanLines dropCR rule.
func refDockerfileLines(data string) []string {
	parts := strings.Split(data, "\n")
	if n := len(parts); n > 0 && parts[n-1] == "" {
		parts = parts[:n-1]
	}
	for i, p := range parts {
		parts[i] = strings.TrimSuffix(p, "\r")
	}
	return parts
}

// FuzzReadDockerfileLines feeds arbitrary (bounded) Dockerfile bytes through
// the package's line reader and checks its line-splitting contract: reads are
// deterministic, no returned line ever contains a newline character, and the
// result matches the reference split exactly (CRLF, lone CR, missing final
// newline and blank-line handling included).
func FuzzReadDockerfileLines(f *testing.F) {
	// Valid seeds.
	f.Add([]byte("FROM golang:1.27.4-bookworm\nUSER 65534:65534\n"))
	f.Add([]byte("FROM scratch\r\nUSER longbow:longbow\r\n")) // CRLF
	f.Add([]byte("no trailing newline"))
	f.Add([]byte("line-one\r")) // lone CR at EOF
	f.Add([]byte("a\n\n\n"))    // blank lines
	// Invalid/edge seeds: empty file, binary junk.
	f.Add([]byte(""))
	f.Add([]byte{0x00, 0x01, 0xff, 0xfe, '\n', 0x7f})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Bound: keep inputs well under bufio.Scanner's 64 KiB token
		// limit so the reader's error path stays out of scope.
		if len(data) > 4096 {
			return
		}

		path := filepath.Join(t.TempDir(), "Dockerfile")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("writing fuzz Dockerfile: %v", err)
		}

		got := readDockerfileLines(t, path)
		again := readDockerfileLines(t, path)
		if len(got) != len(again) {
			t.Fatalf("non-deterministic read: %d vs %d lines", len(got), len(again))
		}
		for i := range got {
			if got[i] != again[i] {
				t.Fatalf("non-deterministic line %d: %q vs %q", i, got[i], again[i])
			}
			if strings.ContainsRune(got[i], '\n') {
				t.Fatalf("line %d contains a newline: %q", i, got[i])
			}
		}

		want := refDockerfileLines(string(data))
		if len(got) != len(want) {
			t.Fatalf("got %d lines, want %d (input %q)", len(got), len(want), data)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("line %d = %q, want %q (input %q)", i, got[i], want[i], data)
			}
		}
	})
}
