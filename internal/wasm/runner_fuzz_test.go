package wasm

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/tetratelabs/wazero"
)

// uleb encodes n as unsigned LEB128.
func uleb(n uint64) []byte {
	var out []byte
	for {
		b := byte(n & 0x7f)
		n >>= 7
		if n != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			return out
		}
	}
}

// wasmSection frames payload with the section id and its LEB128 length.
func wasmSection(id byte, payload []byte) []byte {
	out := append([]byte{id}, uleb(uint64(len(payload)))...)
	return append(out, payload...)
}

// wasmName encodes a section/export name: LEB128 length + raw bytes.
func wasmName(s string) []byte {
	out := uleb(uint64(len(s)))
	return append(out, s...)
}

// buildRoundTripModule assembles a minimal WASM module exposing the exports
// Runner.Inference expects. "malloc" returns a fixed pointer (1024) into the
// zero-initialised 64 KiB memory, "free" is a no-op, and both inference
// functions echo back the (pointer, length) arguments they receive — so any
// bytes the runner writes into WASM memory must come back out unchanged.
func buildRoundTripModule() []byte {
	var mod []byte
	// Magic header + version 1.
	mod = append(mod, 0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00)

	// Type section: (i32)->i32, (i32)->(), (i32,i32)->(i32,i32),
	// (i32,i32,i32)->(i32,i32).
	var types []byte
	types = append(types, 0x04)
	types = append(types, 0x60, 0x01, 0x7f, 0x01, 0x7f)
	types = append(types, 0x60, 0x01, 0x7f, 0x00)
	types = append(types, 0x60, 0x02, 0x7f, 0x7f, 0x02, 0x7f, 0x7f)
	types = append(types, 0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x02, 0x7f, 0x7f)
	mod = append(mod, wasmSection(0x01, types)...)

	// Function section: four functions using the types above in order.
	mod = append(mod, wasmSection(0x03, append([]byte{0x04}, 0x00, 0x01, 0x02, 0x03))...)

	// Memory section: one memory with a single 64 KiB page.
	mod = append(mod, wasmSection(0x05, []byte{0x01, 0x00, 0x01})...)

	// Export section: malloc, free, inference, inference_with_tokens, memory.
	var exports []byte
	exports = append(exports, 0x05)
	exports = append(exports, wasmName("malloc")...)
	exports = append(exports, 0x00, 0x00)
	exports = append(exports, wasmName("free")...)
	exports = append(exports, 0x00, 0x01)
	exports = append(exports, wasmName("inference")...)
	exports = append(exports, 0x00, 0x02)
	exports = append(exports, wasmName("inference_with_tokens")...)
	exports = append(exports, 0x00, 0x03)
	exports = append(exports, wasmName("memory")...)
	exports = append(exports, 0x02, 0x00)
	mod = append(mod, wasmSection(0x07, exports)...)

	// Code section.
	var code []byte
	code = append(code, 0x04)
	// malloc: i32.const 1024 (signed LEB128: 0x80 0x08).
	code = append(code, uleb(5)...)
	code = append(code, 0x00, 0x41, 0x80, 0x08, 0x0b)
	// free: no-op.
	code = append(code, uleb(2)...)
	code = append(code, 0x00, 0x0b)
	// inference(ptr, len) -> (ptr, len).
	code = append(code, uleb(6)...)
	code = append(code, 0x00, 0x20, 0x00, 0x20, 0x01, 0x0b)
	// inference_with_tokens(idPtr, maskPtr, n) -> (maskPtr, n).
	code = append(code, uleb(6)...)
	code = append(code, 0x00, 0x20, 0x01, 0x20, 0x02, 0x0b)
	mod = append(mod, wasmSection(0x0a, code)...)

	return mod
}

// roundTripModule is the shared test module, assembled once.
var roundTripModule = buildRoundTripModule()

// le64 reads the i-th little-endian uint64 from b.
func le64(b []byte, i int) uint64 {
	off := i * 8
	return uint64(b[off]) |
		uint64(b[off+1])<<8 |
		uint64(b[off+2])<<16 |
		uint64(b[off+3])<<24 |
		uint64(b[off+4])<<32 |
		uint64(b[off+5])<<40 |
		uint64(b[off+6])<<48 |
		uint64(b[off+7])<<56
}

// newRoundTripRunner instantiates the shared test module in a fresh runtime.
// The returned cleanup closes the runtime (and the module with it).
func newRoundTripRunner(ctx context.Context, t *testing.T) (*Runner, func()) {
	t.Helper()
	rt := wazero.NewRuntime(ctx)
	mod, err := rt.InstantiateWithConfig(ctx, roundTripModule, wazero.NewModuleConfig())
	if err != nil {
		_ = rt.Close(ctx)
		t.Fatalf("failed to instantiate round-trip module: %v", err)
	}
	return &Runner{runtime: rt, mod: mod, name: "fuzz"}, func() { _ = rt.Close(ctx) }
}

// FuzzRunnerInferenceRoundTrip checks the float32 marshalling of
// Runner.Inference: every input float must round-trip bit-for-bit through
// WASM memory and back, and the output length must match the input length.
func FuzzRunnerInferenceRoundTrip(f *testing.F) {
	// Valid seeds.
	f.Add([]byte{0x00, 0x00, 0x80, 0x3f}) // 1.0f
	f.Add([]byte{})                       // empty input
	f.Add([]byte{0x00, 0x00, 0x80, 0x7f}) // NaN payload bits
	f.Add([]byte{0xff, 0xff, 0xff, 0x7f}) // +Inf bits
	f.Add([]byte{0x00, 0x00, 0x00, 0x80}) // -0.0f
	f.Add(make([]byte, 64))               // zero floats
	// Invalid/edge seed: trailing bytes that do not form a float32.
	f.Add([]byte{1, 2, 3})

	f.Fuzz(func(t *testing.T, input []byte) {
		// Bound: at most 1024 float32s (4 KiB), well within the module's
		// 64 KiB memory when written at offset 1024.
		if len(input) > 4096 {
			return
		}

		ctx := context.Background()
		runner, cleanup := newRoundTripRunner(ctx, t)
		defer cleanup()

		nFloats := len(input) / 4
		want := make([]float32, nFloats)
		for i := range want {
			bits := uint32(input[i*4]) |
				uint32(input[i*4+1])<<8 |
				uint32(input[i*4+2])<<16 |
				uint32(input[i*4+3])<<24
			want[i] = math.Float32frombits(bits)
		}

		got, err := runner.Inference(ctx, want)
		if err != nil {
			t.Fatalf("Inference(%d floats) failed: %v", nFloats, err)
		}
		if len(got) != len(want) {
			t.Fatalf("Inference returned %d floats, want %d", len(got), len(want))
		}
		for i := range got {
			if math.Float32bits(got[i]) != math.Float32bits(want[i]) {
				t.Fatalf("output[%d] bits = %#08x, want %#08x (bit-exact round trip)",
					i, math.Float32bits(got[i]), math.Float32bits(want[i]))
			}
		}
	})
}

// FuzzRunnerTokenInferenceRoundTrip checks the int64 marshalling of
// Runner.InferenceWithTokens. The runner writes token ids and then the mask
// at the same pointer and reads back len(inputIds)*4 bytes, so the output
// must equal the mask's little-endian bytes reinterpreted as float32 values.
func FuzzRunnerTokenInferenceRoundTrip(f *testing.F) {
	// Valid seeds.
	f.Add([]byte{0x01, 0, 0, 0, 0, 0, 0, 0}, []byte{0x02, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{}, []byte{})
	f.Add(make([]byte, 32), make([]byte, 32)) // 4 ids, 4 masks
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, []byte{8, 7, 6, 5, 4, 3, 2, 1})
	f.Add(make([]byte, 8), make([]byte, 16)) // more masks than ids
	// Invalid/edge seeds: mask too small for the read window, oversized
	// mask (would not fit module memory) — both must be skipped.
	f.Add(make([]byte, 32), make([]byte, 8))
	f.Add(make([]byte, 8), make([]byte, 8200))

	f.Fuzz(func(t *testing.T, idBytes, maskBytes []byte) {
		nIDs := len(idBytes) / 8
		nMasks := len(maskBytes) / 8
		// Bounds: at most 1024 ids (8 KiB) fit the module's memory at
		// offset 1024, and the mask write must cover the nIDs*4 bytes the
		// runner reads back.
		if nIDs > 1024 || len(maskBytes) > 8192 || nMasks*8 < nIDs*4 {
			return
		}

		ctx := context.Background()
		runner, cleanup := newRoundTripRunner(ctx, t)
		defer cleanup()

		ids := make([]int64, nIDs)
		for i := range ids {
			ids[i] = int64(le64(idBytes, i))
		}
		masks := make([]int64, nMasks)
		for i := range masks {
			masks[i] = int64(le64(maskBytes, i))
		}

		// Expected output: the mask's little-endian bytes at [4i, 4i+4).
		want := make([]float32, nIDs)
		for i := range want {
			bits := uint32(le64(maskBytes, i/2) >> ((i % 2) * 32))
			want[i] = math.Float32frombits(bits)
		}

		got, err := runner.InferenceWithTokens(ctx, ids, masks)
		if err != nil {
			t.Fatalf("InferenceWithTokens(%d ids, %d masks) failed: %v", nIDs, nMasks, err)
		}
		if len(got) != len(want) {
			t.Fatalf("InferenceWithTokens returned %d floats, want %d", len(got), len(want))
		}
		for i := range got {
			if math.Float32bits(got[i]) != math.Float32bits(want[i]) {
				t.Fatalf("output[%d] bits = %#08x, want %#08x (bit-exact round trip)",
					i, math.Float32bits(got[i]), math.Float32bits(want[i]))
			}
		}
	})
}

// FuzzNewRunnerRejectsGarbage checks that NewRunner fails cleanly — error,
// nil runner, no panic — for arbitrary non-WASM file contents.
func FuzzNewRunnerRejectsGarbage(f *testing.F) {
	// Valid-format-but-garbage seeds.
	f.Add([]byte("this is not a wasm module"))
	f.Add([]byte{})
	f.Add([]byte{0x01, 0x02, 0x03, 0x04})
	f.Add([]byte("GIF89a"))
	f.Add(make([]byte, 64))
	f.Add([]byte{0x00, 0x61, 0x73}) // truncated magic header

	f.Fuzz(func(t *testing.T, data []byte) {
		// Bounds: keep inputs small, and skip anything carrying the WASM
		// magic header — such a module could contain arbitrary code, which
		// is out of scope for this target.
		if len(data) > 4096 {
			return
		}
		if len(data) >= 4 && data[0] == 0x00 && data[1] == 'a' && data[2] == 's' && data[3] == 'm' {
			return
		}

		path := filepath.Join(t.TempDir(), "model.wasm")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("writing fuzz module: %v", err)
		}

		runner, err := NewRunner(context.Background(), path)
		if err == nil {
			_ = runner.Close(context.Background())
			t.Fatalf("NewRunner accepted non-WASM input (%d bytes)", len(data))
		}
		if runner != nil {
			t.Fatalf("NewRunner returned a runner together with error %v", err)
		}
	})
}
