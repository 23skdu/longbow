//go:build linux

package iouring

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func FuzzNextPowerOf2(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(3))
	f.Add(uint32(1023))
	f.Add(uint32(4096))
	f.Add(uint32(65535))
	f.Add(uint32(1) << 31)
	f.Add(uint32(1)<<31 + 1) // overflows 32-bit rounding: out of range, dropped

	f.Fuzz(func(t *testing.T, n uint32) {
		// Beyond 2^31 the bit-smearing rounding overflows uint32; drop those
		// inputs rather than failing on them.
		if n > 1<<31 {
			return
		}

		got := nextPowerOf2(n)

		// A power-of-two rounding must produce a nonzero power of two that
		// is >= n and < 2n (== n exactly when n is already a power of two).
		require.NotZero(t, got)
		require.Zero(t, got&(got-1), "result %d must be a power of two", got)
		require.GreaterOrEqual(t, got, n)
		if n == 0 {
			require.Equal(t, uint32(1), got)
			return
		}
		require.LessOrEqual(t, uint64(got), uint64(n)*2)
		if n&(n-1) == 0 {
			require.Equal(t, n, got, "power of two inputs must be unchanged")
		} else {
			require.Greater(t, got, n, "non-power-of-two inputs must round up")
		}
		// Deterministic across calls.
		require.Equal(t, got, nextPowerOf2(n))
	})
}

func FuzzNewRingEntriesValidation(f *testing.F) {
	f.Add(uint32(0), uint32(0))
	f.Add(uint32(4097), uint32(0))
	f.Add(uint32(8192), uint32(1))
	f.Add(uint32(65536), uint32(7))
	f.Add(uint32(1)<<31, uint32(0))
	f.Add(^uint32(0), uint32(0))
	f.Add(uint32(1), uint32(0))    // valid range: dropped to avoid a real io_uring setup
	f.Add(uint32(4096), uint32(0)) // valid range: dropped

	f.Fuzz(func(t *testing.T, entries, flags uint32) {
		if entries >= 1 && entries <= 4096 {
			// Valid entries would invoke io_uring_setup(2); that syscall is
			// out of scope for fuzzing, so drop the input.
			return
		}

		// Invalid entry counts must be rejected up front, deterministically,
		// without touching the kernel.
		ring1, err1 := NewRing(entries, flags)
		require.Nil(t, ring1)
		require.ErrorContains(t, err1, "invalid entries")

		ring2, err2 := NewRing(entries, flags)
		require.Nil(t, ring2)
		require.EqualError(t, err1, err2.Error())
	})
}

func FuzzBufferPoolLifecycle(f *testing.F) {
	f.Add(512, 2, byte(0xAB))
	f.Add(1, 1, byte(255))
	f.Add(4096, 4, byte(1))
	f.Add(0, 1, byte(0))       // invalid buffer size: dropped by validation
	f.Add(65536, 1, byte(9))   // exactly one page (boundary)
	f.Add(-1, 2, byte(3))      // invalid buffer size: dropped by validation
	f.Add(100, 0, byte(4))     // invalid buffer count: dropped by validation
	f.Add(60000, 3, byte(127)) // rounds up to a page multiple

	f.Fuzz(func(t *testing.T, bufferSize, maxBuffers int, marker byte) {
		// Bound allocations: keep mmaps small regardless of fuzzed sizes.
		if bufferSize > 64*1024 || maxBuffers > 4 {
			return
		}

		pool, err := NewBufferPool(bufferSize, maxBuffers)
		if bufferSize <= 0 || maxBuffers <= 0 {
			// Validation: non-positive arguments must be rejected before
			// any allocation happens.
			require.Error(t, err)
			require.Nil(t, pool)
			return
		}
		require.NoError(t, err)
		require.NotNil(t, pool)

		// Buffer size is rounded up to a page multiple, alignment pinned.
		want := ((bufferSize + pageSize - 1) / pageSize) * pageSize
		require.Equal(t, want, pool.bufferSize)
		require.Equal(t, DefaultAlignment, pool.alignment)

		avail, allocd := pool.Stats()
		require.Equal(t, maxBuffers, avail)
		require.Equal(t, 0, allocd)

		// Drain the pool completely.
		held := make([][]byte, 0, maxBuffers)
		for i := 0; i < maxBuffers; i++ {
			buf := pool.Get()
			require.NotNil(t, buf, "buffer %d must be available", i)
			require.Len(t, buf, want)
			require.True(t, pool.IsAligned(buf), "buffer %d must be aligned", i)
			require.Zero(t, uintptr(unsafe.Pointer(&buf[0]))%uintptr(DefaultAlignment))

			// The mapped buffer must be writable and stable (round trip).
			buf[0] = marker
			buf[want-1] = marker ^ 0xFF
			require.Equal(t, marker, buf[0])
			require.Equal(t, marker^0xFF, buf[want-1])

			held = append(held, buf)
			avail, allocd = pool.Stats()
			require.Equal(t, maxBuffers-i-1, avail)
			require.Equal(t, i+1, allocd)
		}

		// An exhausted pool must return nil instead of blocking.
		require.Nil(t, pool.Get())

		// Return half of the buffers; accounting must mirror it exactly.
		putBack := (maxBuffers + 1) / 2
		for i := 0; i < putBack; i++ {
			pool.Put(held[i])
		}
		avail, allocd = pool.Stats()
		require.Equal(t, putBack, avail)
		require.Equal(t, maxBuffers-putBack, allocd)

		// A recycled buffer comes back with the same shape and alignment.
		recycled := pool.Get()
		require.NotNil(t, recycled)
		require.Len(t, recycled, want)
		require.True(t, pool.IsAligned(recycled))
		pool.Put(recycled)

		// Release everything still held; the pool must end fully accounted.
		for i := putBack; i < maxBuffers; i++ {
			pool.Put(held[i])
		}
		avail, allocd = pool.Stats()
		require.Equal(t, maxBuffers, avail)
		require.Equal(t, 0, allocd)

		require.NoError(t, pool.Close())
		avail, _ = pool.Stats()
		require.Equal(t, 0, avail, "closed pool must be drained")
	})
}
