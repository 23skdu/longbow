//go:build linux

package iouring

import (
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRing(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)
	defer ring.Close()

	assert.Greater(t, ring.fd, 0)
	assert.NotNil(t, ring.sqRingArea)
	assert.NotNil(t, ring.sqesArea)
	assert.True(t, ring.sqEntriesCached > 0)
	assert.True(t, ring.cqEntriesCached > 0)
}

func TestNewRingInvalidEntries(t *testing.T) {
	// Zero entries
	ring, err := NewRing(0, 0)
	assert.Error(t, err)
	assert.Nil(t, ring)

	// Too many entries
	ring, err = NewRing(8192, 0)
	assert.Error(t, err)
	assert.Nil(t, ring)
}

func TestRingClose(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)

	err = ring.Close()
	assert.NoError(t, err)
	assert.Equal(t, -1, ring.fd)
}

func TestRingCloseMultiple(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)

	// Should be safe to close multiple times
	err = ring.Close()
	assert.NoError(t, err)

	err = ring.Close()
	assert.NoError(t, err)
}

func TestMmapRings(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)
	defer ring.Close()

	// Verify SQ pointers are set
	assert.NotNil(t, ring.sqHead)
	assert.NotNil(t, ring.sqTail)
	assert.NotNil(t, ring.sqRingMask)
	assert.NotNil(t, ring.sqRingEntries)
	assert.NotNil(t, ring.sqFlags)
	assert.NotNil(t, ring.sqDropped)
	assert.NotNil(t, ring.sqArray)

	// Verify CQ pointers are set
	assert.NotNil(t, ring.cqHead)
	assert.NotNil(t, ring.cqTail)
	assert.NotNil(t, ring.cqRingMask)
	assert.NotNil(t, ring.cqRingEntries)
	assert.NotNil(t, ring.cqOverflow)
	assert.NotNil(t, ring.cqFlags)
	assert.NotNil(t, ring.cqes)

	// Verify SQEs array
	assert.NotNil(t, ring.sqes)
	assert.Equal(t, int(ring.params.SqEntries), len(ring.sqes))
}

func TestSubmitAndComplete(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)
	defer ring.Close()

	// Create a temp file for testing
	tmpfile, err := os.CreateTemp("", "iouring-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Prepare write SQE
	data := []byte("hello io_uring")
	sqe := &SQE{
		Opcode:   IORING_OP_WRITE,
		Fd:       int32(tmpfile.Fd()),
		Addr:     uint64(uintptr(unsafe.Pointer(&data[0]))),
		Len:      uint32(len(data)),
		Off:      0,
		UserData: 42,
	}

	// Submit
	err = ring.Submit(sqe)
	require.NoError(t, err)

	// Flush to kernel
	submitted, err := ring.Flush()
	require.NoError(t, err)
	assert.Equal(t, 1, submitted)

	// Wait for completion
	cqe, err := ring.Wait()
	require.NoError(t, err)
	require.NotNil(t, cqe)

	// Verify completion
	assert.Equal(t, uint64(42), cqe.UserData)
	assert.Equal(t, int32(len(data)), cqe.Res)

	// Advance CQ
	ring.Advance(1)

	// Verify data was written
	content, err := os.ReadFile(tmpfile.Name())
	require.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestSubmitRead(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)
	defer ring.Close()

	// Create a temp file with data
	tmpfile, err := os.CreateTemp("", "iouring-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	writeData := []byte("test data for reading")
	_, err = tmpfile.Write(writeData)
	require.NoError(t, err)
	_, err = tmpfile.Seek(0, 0)
	require.NoError(t, err)

	// Prepare read buffer
	readBuf := make([]byte, len(writeData))

	// Submit read
	err = ring.SubmitRead(int(tmpfile.Fd()), readBuf, 0, 100)
	require.NoError(t, err)

	// Flush
	_, err = ring.Flush()
	require.NoError(t, err)

	// Wait for completion
	cqe, err := ring.Wait()
	require.NoError(t, err)
	require.NotNil(t, cqe)

	// Verify
	assert.Equal(t, uint64(100), cqe.UserData)
	assert.Equal(t, int32(len(writeData)), cqe.Res)
	assert.Equal(t, writeData, readBuf)
}

func TestSubmitWrite(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)
	defer ring.Close()

	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Submit write
	data := []byte("direct write test")
	err = ring.SubmitWrite(int(tmpfile.Fd()), data, 0, 200)
	require.NoError(t, err)

	// Flush and wait
	_, err = ring.Flush()
	require.NoError(t, err)

	cqe, err := ring.Wait()
	require.NoError(t, err)
	assert.Equal(t, int32(len(data)), cqe.Res)

	// Verify file content
	content, err := os.ReadFile(tmpfile.Name())
	require.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestSubmitFsync(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)
	defer ring.Close()

	// Create temp file
	tmpfile, err := os.CreateTemp("", "iouring-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Write some data
	data := []byte("data to sync")
	_, err = tmpfile.Write(data)
	require.NoError(t, err)

	// Submit fsync
	err = ring.SubmitFsync(int(tmpfile.Fd()), false, 300)
	require.NoError(t, err)

	// Flush and wait
	_, err = ring.Flush()
	require.NoError(t, err)

	cqe, err := ring.Wait()
	require.NoError(t, err)
	assert.Equal(t, int32(0), cqe.Res) // fsync returns 0 on success
}

func TestSqSpaceLeft(t *testing.T) {
	ring, err := NewRing(4, 0) // Small ring for testing
	require.NoError(t, err)
	defer ring.Close()

	// Initially should be empty
	assert.Equal(t, uint32(4), ring.SqSpaceLeft())
	assert.Equal(t, uint32(0), ring.SqReady())

	// Submit one entry
	sqe := &SQE{
		Opcode: IORING_OP_NOP,
		Fd:     -1,
	}
	err = ring.Submit(sqe)
	require.NoError(t, err)

	assert.Equal(t, uint32(3), ring.SqSpaceLeft())
	assert.Equal(t, uint32(1), ring.SqReady())
}

func TestSubmitFullQueue(t *testing.T) {
	ring, err := NewRing(2, 0) // Very small ring
	require.NoError(t, err)
	defer ring.Close()

	// Fill the queue
	for i := 0; i < 2; i++ {
		sqe := &SQE{
			Opcode: IORING_OP_NOP,
			Fd:     -1,
		}
		err = ring.Submit(sqe)
		require.NoError(t, err)
	}

	// Next submit should fail
	sqe := &SQE{
		Opcode: IORING_OP_NOP,
		Fd:     -1,
	}
	err = ring.Submit(sqe)
	assert.Equal(t, ErrRingFull, err)
}

func TestCqReady(t *testing.T) {
	ring, err := NewRing(64, 0)
	require.NoError(t, err)
	defer ring.Close()

	// Initially empty
	assert.Equal(t, uint32(0), ring.CqReady())

	// Create a temp file for testing
	tmpfile, err := os.CreateTemp("", "iouring-test-")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Submit a nop operation
	sqe := &SQE{
		Opcode: IORING_OP_NOP,
		Fd:     -1,
	}
	err = ring.Submit(sqe)
	require.NoError(t, err)

	_, err = ring.Flush()
	require.NoError(t, err)

	// Wait for completion
	_, err = ring.Wait()
	require.NoError(t, err)

	// Should have one completion ready
	assert.Equal(t, uint32(1), ring.CqReady())
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct {
		input    uint32
		expected uint32
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{16, 16},
		{17, 32},
		{1023, 1024},
		{1024, 1024},
	}

	for _, tc := range tests {
		result := nextPowerOf2(tc.input)
		assert.Equal(t, tc.expected, result, "input: %d", tc.input)
	}
}

func TestRingFinalizer(t *testing.T) {
	// This test ensures the finalizer doesn't panic
	ring, err := NewRing(64, 0)
	require.NoError(t, err)

	// Close explicitly to avoid leak
	err = ring.Close()
	require.NoError(t, err)
}

// Benchmarks

func BenchmarkSubmit(b *testing.B) {
	ring, err := NewRing(4096, 0)
	require.NoError(b, err)
	defer ring.Close()

	sqe := &SQE{
		Opcode: IORING_OP_NOP,
		Fd:     -1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear queue periodically to avoid overflow
		if i%1024 == 0 {
			ring.Flush()
		}

		err := ring.Submit(sqe)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubmitAndFlush(b *testing.B) {
	ring, err := NewRing(4096, 0)
	require.NoError(b, err)
	defer ring.Close()

	sqe := &SQE{
		Opcode: IORING_OP_NOP,
		Fd:     -1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := ring.Submit(sqe)
		if err != nil {
			b.Fatal(err)
		}

		if i%128 == 0 {
			ring.Flush()
		}
	}
}
