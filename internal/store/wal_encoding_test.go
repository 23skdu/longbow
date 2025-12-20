package store

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// =============================================================================
// WAL Binary Encoding Tests - TDD for reflection elimination
// =============================================================================

// TestEncodeUint32_Manual verifies manual uint32 encoding matches binary.Write
func TestEncodeUint32_Manual(t *testing.T) {
	testCases := []uint32{0, 1, 255, 256, 65535, 65536, 0xFFFFFFFF}

	for _, val := range testCases {
		// Manual encoding (what we want to use)
		manualBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(manualBuf, val)

		// binary.Write encoding (what we're replacing)
		reflectBuf := new(bytes.Buffer)
		if err := binary.Write(reflectBuf, binary.LittleEndian, val); err != nil {
			t.Fatalf("binary.Write failed: %v", err)
		}

		if !bytes.Equal(manualBuf, reflectBuf.Bytes()) {
			t.Errorf("uint32 %d: manual=%v, reflect=%v", val, manualBuf, reflectBuf.Bytes())
		}
	}
}

// TestEncodeUint64_Manual verifies manual uint64 encoding matches binary.Write
func TestEncodeUint64_Manual(t *testing.T) {
	testCases := []uint64{0, 1, 255, 256, 65535, 65536, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF}

	for _, val := range testCases {
		// Manual encoding
		manualBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(manualBuf, val)

		// binary.Write encoding
		reflectBuf := new(bytes.Buffer)
		if err := binary.Write(reflectBuf, binary.LittleEndian, val); err != nil {
			t.Fatalf("binary.Write failed: %v", err)
		}

		if !bytes.Equal(manualBuf, reflectBuf.Bytes()) {
			t.Errorf("uint64 %d: manual=%v, reflect=%v", val, manualBuf, reflectBuf.Bytes())
		}
	}
}

// TestWALHeaderEncoding tests the complete WAL entry header encoding
func TestWALHeaderEncoding(t *testing.T) {
	nameLen := uint32(12)  // "test-dataset"
	recLen := uint64(1024) // 1KB record
	crc := uint32(0x12345678)

	// Manual encoding (optimized path)
	headerBuf := make([]byte, 16) // 4 bytes CRC + 4 bytes uint32 + 8 bytes uint64
	binary.LittleEndian.PutUint32(headerBuf[0:4], crc)
	binary.LittleEndian.PutUint32(headerBuf[4:8], nameLen)
	binary.LittleEndian.PutUint64(headerBuf[8:16], recLen)

	// Verify decoding roundtrip
	decodedCrc := binary.LittleEndian.Uint32(headerBuf[0:4])
	decodedNameLen := binary.LittleEndian.Uint32(headerBuf[4:8])
	decodedRecLen := binary.LittleEndian.Uint64(headerBuf[8:16])

	if decodedCrc != crc {
		t.Errorf("crc mismatch: got %d, want %d", decodedCrc, crc)
	}
	if decodedNameLen != nameLen {
		t.Errorf("nameLen mismatch: got %d, want %d", decodedNameLen, nameLen)
	}
	if decodedRecLen != recLen {
		t.Errorf("recLen mismatch: got %d, want %d", decodedRecLen, recLen)
	}
}

// TestEncodeWALEntryHeader tests the helper function we will create
func TestEncodeWALEntryHeader(t *testing.T) {
	tests := []struct {
		name    string
		crc     uint32
		nameLen uint32
		recLen  uint64
	}{
		{"zero values", 0, 0, 0},
		{"small values", 1, 10, 100},
		{"medium values", 12345, 1000, 1000000},
		{"large name", 99999, 0xFFFFFFFF, 100},
		{"large record", 88888, 100, 0xFFFFFFFFFFFFFFFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := encodeWALEntryHeader(tt.crc, tt.nameLen, tt.recLen)

			if len(buf) != 16 {
				t.Fatalf("expected 16 bytes, got %d", len(buf))
			}

			// Verify we can decode it back
			gotCrc := binary.LittleEndian.Uint32(buf[0:4])
			gotNameLen := binary.LittleEndian.Uint32(buf[4:8])
			gotRecLen := binary.LittleEndian.Uint64(buf[8:16])

			if gotCrc != tt.crc {
				t.Errorf("crc: got %d, want %d", gotCrc, tt.crc)
			}
			if gotNameLen != tt.nameLen {
				t.Errorf("nameLen: got %d, want %d", gotNameLen, tt.nameLen)
			}
			if gotRecLen != tt.recLen {
				t.Errorf("recLen: got %d, want %d", gotRecLen, tt.recLen)
			}
		})
	}
}

// =============================================================================
// Benchmarks - Verify performance improvement
// =============================================================================

// BenchmarkEncodeUint32_Reflect benchmarks binary.Write (reflection-based)
func BenchmarkEncodeUint32_Reflect(b *testing.B) {
	buf := new(bytes.Buffer)
	val := uint32(12345)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, val)
	}
}

// BenchmarkEncodeUint32_Manual benchmarks manual encoding (no reflection)
func BenchmarkEncodeUint32_Manual(b *testing.B) {
	buf := make([]byte, 4)
	val := uint32(12345)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint32(buf, val)
	}
}

// BenchmarkEncodeUint64_Reflect benchmarks binary.Write (reflection-based)
func BenchmarkEncodeUint64_Reflect(b *testing.B) {
	buf := new(bytes.Buffer)
	val := uint64(1234567890)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, val)
	}
}

// BenchmarkEncodeUint64_Manual benchmarks manual encoding (no reflection)
func BenchmarkEncodeUint64_Manual(b *testing.B) {
	buf := make([]byte, 8)
	val := uint64(1234567890)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(buf, val)
	}
}

// BenchmarkEncodeWALHeader_Reflect benchmarks full header with reflection
func BenchmarkEncodeWALHeader_Reflect(b *testing.B) {
	buf := new(bytes.Buffer)
	crc := uint32(12345)
	nameLen := uint32(12)
	recLen := uint64(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = binary.Write(buf, binary.LittleEndian, crc)
		_ = binary.Write(buf, binary.LittleEndian, nameLen)
		_ = binary.Write(buf, binary.LittleEndian, recLen)
	}
}

// BenchmarkEncodeWALHeader_Manual benchmarks full header without reflection
func BenchmarkEncodeWALHeader_Manual(b *testing.B) {
	headerBuf := make([]byte, 16)
	crc := uint32(12345)
	nameLen := uint32(12)
	recLen := uint64(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint32(headerBuf[0:4], crc)
		binary.LittleEndian.PutUint32(headerBuf[4:8], nameLen)
		binary.LittleEndian.PutUint64(headerBuf[8:16], recLen)
	}
}

// BenchmarkEncodeWALEntryHeader benchmarks our helper function
func BenchmarkEncodeWALEntryHeader(b *testing.B) {
	crc := uint32(12345)
	nameLen := uint32(12)
	recLen := uint64(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encodeWALEntryHeader(crc, nameLen, recLen)
	}
}
