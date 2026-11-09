package tuning

import (
	"math"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// decodeFloat64s reinterprets fuzz bytes as little-endian float64 values.
// Returns false when the input is outside the supported bounds so callers can
// skip out-of-range inputs.
func decodeFloat64s(b []byte, maxCount int, maxAbs float64) ([]float64, bool) {
	n := len(b) / 8
	if n > maxCount {
		return nil, false
	}
	out := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		bits := uint64(b[i*8]) |
			uint64(b[i*8+1])<<8 |
			uint64(b[i*8+2])<<16 |
			uint64(b[i*8+3])<<24 |
			uint64(b[i*8+4])<<32 |
			uint64(b[i*8+5])<<40 |
			uint64(b[i*8+6])<<48 |
			uint64(b[i*8+7])<<56
		v := math.Float64frombits(bits)
		if math.IsNaN(v) || math.IsInf(v, 0) || v > maxAbs || v < -maxAbs {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

// encodeF64s packs float64 values as little-endian bytes for seed corpora.
func encodeF64s(vals ...float64) []byte {
	out := make([]byte, 0, len(vals)*8)
	for _, v := range vals {
		bits := math.Float64bits(v)
		for s := uint(0); s < 64; s += 8 {
			out = append(out, byte(bits>>s))
		}
	}
	return out
}

// FuzzAdjustParameterBounds checks the clamping contract of
// AutoTuner.adjustParameter: with finite, bounded inputs the adjusted value
// must stay inside [min,max] for a valid range, must land exactly on max for
// an inverted (invalid) range, and must not move at all while there are fewer
// than MinSamples samples. MinSamples itself is fuzzed across [1,32] so the
// gradient estimate runs with histories both shorter and longer than its
// 10-sample window (a short history previously panicked here).
func FuzzAdjustParameterBounds(f *testing.F) {
	// Valid seeds (MinSamples=10: the package default).
	f.Add(0.0, 100.0, 0.1, 10, encodeF64s(50, 60, 55)) // fewer than minSamples
	f.Add(0.0, 1.0, 0.5, 10, encodeF64s(0.1, 0.9, 0.5, 0.4, 0.6, 0.7, 0.8, 0.2, 0.3, 0.4, 0.5, 0.6))
	f.Add(10.0, 10.0, 0.1, 10, encodeF64s(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)) // min == max
	f.Add(-1e6, 1e6, 0.01, 10, encodeF64s(-5e5, 5e5, -4e5, 4e5, -3e5, 3e5, -2e5, 2e5, -1e5, 1e5, 0, 1e5))
	// Short-history seeds: MinSamples below the 10-sample gradient window.
	// (0.0, 100.0, 0.1, 5, 5 samples) is the regression seed for the
	// estimateGradient negative-slice-bound panic.
	f.Add(0.0, 100.0, 0.1, 5, encodeF64s(1, 2, 3, 4, 5))
	f.Add(0.0, 100.0, 0.1, 2, encodeF64s(1, 2))
	f.Add(0.0, 100.0, 0.1, 9, encodeF64s(1, 2, 3, 4, 5, 6, 7, 8, 9))
	// Invalid seeds: inverted range, no samples, NaN bound.
	f.Add(100.0, 0.0, 0.1, 10, encodeF64s(42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52))
	f.Add(0.0, 100.0, 0.1, 10, []byte(nil))
	f.Add(math.NaN(), 0.0, 0.1, 10, encodeF64s(1))

	f.Fuzz(func(t *testing.T, minVal, maxVal, learningRate float64, minSamples int, histBytes []byte) {
		// Bound: skip non-finite or excessively large parameters, and keep
		// MinSamples in a sane range (0 would be defaulted to 10 by config).
		for _, v := range []float64{minVal, maxVal, learningRate} {
			if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 1e9 {
				return
			}
		}
		if minSamples < 1 || minSamples > 32 {
			return
		}
		history, ok := decodeFloat64s(histBytes, 128, 1e9)
		if !ok {
			return
		}

		logger := zerolog.Nop()
		tuner := NewAutoTuner(AutoTunerConfig{
			Enabled:        false, // never started: no goroutines
			SampleInterval: time.Second,
			MinSamples:     minSamples,
			Confidence:     0.95,
			LearningRate:   learningRate,
		}, &logger)
		param := tuner.RegisterParameter("fuzz_param", minVal, maxVal, TuningTypeContinuous)

		initial := (minVal + maxVal) / 2
		if got := param.value.Load().(float64); got != initial {
			t.Fatalf("initial value = %v, want midpoint %v", got, initial)
		}
		param.history = append(param.history, history...)

		tuner.adjustParameter(param)
		got := param.value.Load().(float64)

		switch {
		case len(history) < minSamples:
			// Without enough samples the value must remain untouched.
			if got != initial {
				t.Fatalf("value changed with only %d samples: got %v, want %v", len(history), got, initial)
			}
		case minVal <= maxVal:
			// The adjusted value must stay within the configured bounds.
			if got < minVal || got > maxVal {
				t.Fatalf("adjusted value %v outside [%v, %v]", got, minVal, maxVal)
			}
		default:
			// Invalid (inverted) ordering: clamping must land on maxVal.
			if got != maxVal {
				t.Fatalf("clamped value %v, want maxVal %v for inverted range [%v, %v]",
					got, maxVal, minVal, maxVal)
			}
		}
	})
}

// FuzzWorkloadAnalyzeClassification checks that WorkloadAnalyzer.Analyze is
// deterministic and that the emitted classification always agrees with the
// documented average-query-rate thresholds (empty -> unknown, >5000 ->
// throughput, <100 -> latency, otherwise balanced).
func FuzzWorkloadAnalyzeClassification(f *testing.F) {
	// Valid seeds.
	f.Add(encodeF64s()) // empty: unknown
	f.Add(encodeF64s(10, 20, 30))
	f.Add(encodeF64s(6000, 7000))
	f.Add(encodeF64s(2000, 3000, 4000))
	// Invalid/edge seeds: negative rates, exact threshold values.
	f.Add(encodeF64s(-100, -1))
	f.Add(encodeF64s(100))
	f.Add(encodeF64s(5000))

	f.Fuzz(func(t *testing.T, rateBytes []byte) {
		// Bound: at most 1000 samples (the analyzer's default cap), each
		// with a magnitude that cannot overflow the running sum.
		rates, ok := decodeFloat64s(rateBytes, 1000, 1e12)
		if !ok {
			return
		}

		w := NewWorkloadAnalyzer(0) // default cap of 1000 samples
		for _, r := range rates {
			w.RecordSample(r, 0, 0, 0)
		}

		first := w.Analyze()
		second := w.Analyze()
		if first != second {
			t.Fatalf("Analyze not deterministic: %+v vs %+v", first, second)
		}

		if len(rates) == 0 {
			if first.Classification != "unknown" {
				t.Fatalf("empty profile classified %q, want unknown", first.Classification)
			}
			return
		}

		var sum float64
		for _, r := range rates {
			sum += r
		}
		avg := sum / float64(len(rates))
		if first.AverageQueryRate != avg {
			t.Fatalf("AverageQueryRate = %v, want %v", first.AverageQueryRate, avg)
		}

		want := "balanced"
		if avg > 5000 {
			want = "throughput"
		} else if avg < 100 {
			want = "latency"
		}
		if first.Classification != want {
			t.Fatalf("classification %q for average %v, want %q", first.Classification, avg, want)
		}
	})
}
