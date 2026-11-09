package version

import (
	"runtime"
	"strings"
	"testing"
)

// FuzzInfoRoundTrip checks that Info() renders the package version fields
// losslessly: for single-line, bounded field values the output parses back
// into exactly five lines, each carrying its label prefix followed by the
// exact field value.
func FuzzInfoRoundTrip(f *testing.F) {
	// Valid seeds.
	f.Add("0.2.4", "abc1234", "2026-01-01")
	f.Add("", "", "")
	f.Add("v1.2.3-rc.1+build.5", "none", "unknown")
	f.Add("λ-версия1.0", "提交deadbeef", "日期 2026")
	// Invalid seeds (out of range): embedded newline, oversized value.
	f.Add("bad\nversion", "c", "d")
	f.Add(strings.Repeat("x", 600), "c", "d")

	f.Fuzz(func(t *testing.T, ver, commit, buildDate string) {
		// Bound: single-line, reasonably sized fields only; a newline in
		// a field would make the output ambiguous by design.
		if len(ver) > 512 || len(commit) > 512 || len(buildDate) > 512 {
			return
		}
		if strings.ContainsAny(ver, "\r\n") ||
			strings.ContainsAny(commit, "\r\n") ||
			strings.ContainsAny(buildDate, "\r\n") {
			return
		}

		// Swap in the fuzzed fields and restore them afterwards so other
		// tests always observe the package defaults.
		origVer, origCommit, origDate := Version, Commit, BuildDate
		defer func() { Version, Commit, BuildDate = origVer, origCommit, origDate }()
		Version, Commit, BuildDate = ver, commit, buildDate

		info := Info()
		lines := strings.Split(info, "\n")
		if len(lines) != 5 {
			t.Fatalf("Info() produced %d lines, want 5: %q", len(lines), info)
		}

		prefixes := []string{
			"Longbow version ",
			"Commit: ",
			"BuildDate: ",
			"GoVersion: ",
			"OS/Arch: ",
		}
		fields := []string{
			ver,
			commit,
			buildDate,
			runtime.Version(),
			runtime.GOOS + "/" + runtime.GOARCH,
		}
		for i := range prefixes {
			if !strings.HasPrefix(lines[i], prefixes[i]) {
				t.Fatalf("line %d = %q, want prefix %q", i, lines[i], prefixes[i])
			}
			if got := strings.TrimPrefix(lines[i], prefixes[i]); got != fields[i] {
				t.Fatalf("line %d field = %q, want %q", i, got, fields[i])
			}
		}
	})
}
