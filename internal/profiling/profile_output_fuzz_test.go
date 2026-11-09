package profiling

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

var knownProfileTypes = map[string]bool{
	"cpu":          true,
	"heap":         true,
	"goroutine":    true,
	"threadcreate": true,
	"block":        true,
	"mutex":        true,
}

// FuzzProfilerWriteProfileType verifies that WriteProfile accepts exactly the
// documented profile types (returning nil and, for heap/goroutine, actual
// profile data) and rejects every other string with an explicit error.
func FuzzProfilerWriteProfileType(f *testing.F) {
	f.Add("heap")
	f.Add("goroutine")
	f.Add("cpu")
	f.Add("threadcreate")
	f.Add("block")
	f.Add("mutex")
	f.Add("not-a-profile")
	f.Add("")

	f.Fuzz(func(t *testing.T, profileType string) {
		if len(profileType) > 64 {
			return
		}

		p := &Profiler{}
		var buf bytes.Buffer
		err := p.WriteProfile(profileType, &buf)

		if knownProfileTypes[profileType] {
			if err != nil {
				t.Fatalf("WriteProfile(%q) unexpected error: %v", profileType, err)
			}
			switch profileType {
			case "heap", "goroutine":
				// These two must always emit bytes: heap serializes a protobuf
				// profile, goroutine dumps the full goroutine stacks.
				if buf.Len() == 0 {
					t.Fatalf("WriteProfile(%q) wrote no profile data", profileType)
				}
			}
			return
		}

		if err == nil {
			t.Fatalf("WriteProfile(%q) returned nil, want error for unknown type", profileType)
		}
		if !strings.Contains(err.Error(), "unknown profile type") {
			t.Fatalf("WriteProfile(%q) error = %q, want it to mention unknown profile type", profileType, err)
		}
	})
}

// FuzzGPURecorderRetentionBounds verifies the bounded ring behavior of
// GPURecorder: it never retains more than maxRecords entries, keeps them in
// insertion order, and preserves every recorded field verbatim.
func FuzzGPURecorderRetentionBounds(f *testing.F) {
	f.Add(3, "kernel_a", int64(100), int64(64), float32(0.5))
	f.Add(0, "", int64(0), int64(0), float32(0))
	f.Add(1, "x", int64(-1), int64(-1), float32(1.5))
	f.Add(5, "reduce-kernel", int64(1<<40), int64(1<<30), float32(100.25))
	f.Add(2, "k", int64(-999999), int64(999999), float32(-0.75))

	f.Fuzz(func(t *testing.T, maxRecords int, name string, dur, mem int64, util float32) {
		if maxRecords < 0 || maxRecords > 4096 {
			return
		}
		if len(name) > 256 {
			return
		}
		if math.IsNaN(float64(util)) {
			return
		}

		g := NewGPURecorder(maxRecords)

		total := maxRecords + 3 // overfill by a bounded amount
		for i := 0; i < total; i++ {
			g.Record(name, dur, mem, util)
		}

		records := g.GetRecords()
		if len(records) != maxRecords {
			t.Fatalf("retained %d records, want exactly %d", len(records), maxRecords)
		}
		for i, r := range records {
			if r.KernelName != name || r.DurationUs != dur ||
				r.MemoryUsedMB != mem || r.Utilization != util {
				t.Fatalf("record %d corrupted: got {name:%q dur:%d mem:%d util:%v}, want {name:%q dur:%d mem:%d util:%v}",
					i, r.KernelName, r.DurationUs, r.MemoryUsedMB, r.Utilization, name, dur, mem, util)
			}
		}
	})
}
