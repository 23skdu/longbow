package memory

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func FuzzDoubleBufferWriteSwap(f *testing.F) {
	f.Add(64, []byte{0, 0, 1, 0, 2, 3, 1, 0}, []byte("hello double buffer"))
	f.Add(10, []byte{0, 0, 0, 0}, []byte("overflowing payload")) // capacity exceeded path
	f.Add(0, []byte{0, 3}, []byte{1, 2, 3})                      // zero capacity: every write fails
	f.Add(128, []byte{1, 1, 2, 0, 0, 3}, []byte{0xde, 0xad, 0xbe, 0xef})
	f.Add(1, []byte{0, 1, 0, 1, 2, 0, 3}, []byte{})
	f.Add(1024, []byte{0, 3, 1, 0}, make([]byte, 900))
	f.Add(-1, []byte{0}, []byte{1})        // negative capacity: out of range, dropped
	f.Add(8, make([]byte, 200), []byte{7}) // too many ops: dropped

	f.Fuzz(func(t *testing.T, capacity int, ops, payload []byte) {
		// Bound inputs; out-of-range values are dropped, never failed on.
		if capacity < 0 || capacity > 1024 {
			return
		}
		if len(ops) > 64 || len(payload) > 4096 {
			return
		}

		db := NewDoubleBuffer(capacity)

		// Reference model of both slabs plus the active-buffer bookkeeping.
		bufA := make([]byte, capacity)
		bufB := make([]byte, capacity)
		active := 0 // 0 = bufA, 1 = bufB
		activeSize := 0

		cur := 0 // cursor into payload for write chunks
		for step, b := range ops {
			switch b % 4 {
			case 0: // write
				if len(payload) == 0 {
					continue
				}
				n := int(b>>2)%8 + 1
				if n > len(payload) {
					n = len(payload)
				}
				if cur+n > len(payload) {
					cur = 0
				}
				chunk := payload[cur : cur+n]
				cur += n

				written, err := db.Write(chunk)
				if activeSize+n > capacity {
					require.Errorf(t, err, "step %d: write beyond capacity must fail", step)
					require.Equal(t, activeSize, db.ActiveSize(), "step %d: failed write must not change active size", step)
					continue
				}
				require.NoErrorf(t, err, "step %d: in-capacity write must succeed", step)
				require.Equal(t, n, written, "step %d: write must report len(data)", step)
				activeSize += n
				if active == 0 {
					copy(bufA[activeSize-n:], chunk)
				} else {
					copy(bufB[activeSize-n:], chunk)
				}

			case 1: // swap
				db.Swap()
				active = 1 - active
				activeSize = 0

			case 2: // reset
				db.Reset()
				activeSize = 0

			case 3: // verify
				require.Equal(t, activeSize, db.ActiveSize(), "step %d: active size mismatch", step)
				wantActive, wantInactive := bufA, bufB
				if active == 1 {
					wantActive, wantInactive = bufB, bufA
				}
				require.Equal(t, wantActive, db.GetActive(), "step %d: active slab content mismatch", step)
				require.Equal(t, wantInactive, db.GetInactive(), "step %d: inactive slab content mismatch", step)
			}
		}

		// Final model parity.
		require.Equal(t, capacity, db.Capacity())
		require.Equal(t, activeSize, db.ActiveSize())
		wantActive, wantInactive := bufA, bufB
		if active == 1 {
			wantActive, wantInactive = bufB, bufA
		}
		require.Equal(t, wantActive, db.GetActive())
		require.Equal(t, wantInactive, db.GetInactive())
		if capacity > 0 {
			// The two slabs must be distinct backing arrays.
			require.False(t, &db.GetActive()[0] == &db.GetInactive()[0])
		}
	})
}

func FuzzDoubleBufferHeadroom(f *testing.F) {
	f.Add(200, int64(1000), int64(200), int64(100), int64(400))           // fits: 500 <= 800
	f.Add(400, int64(1000), int64(200), int64(100), int64(750))           // check fails: 850 > 800
	f.Add(400, int64(1000), int64(200), int64(100), int64(400))           // construct fails: 900 > 800
	f.Add(0, int64(1000), int64(0), int64(0), int64(0))                   // capacity must be positive
	f.Add(-5, int64(1000), int64(0), int64(0), int64(0))                  // capacity must be positive
	f.Add(512, int64(0), int64(0), int64(100000000), int64(1000000))      // totalBytes 0 = unlimited headroom
	f.Add(1024, int64(1000000000), int64(1000000000), int64(0), int64(1)) // no free memory left
	f.Add(8, int64(500), int64(100), int64(50), int64(1))

	f.Fuzz(func(t *testing.T, capacity int, totalBytes, usedBytes, minHeadroom, requiredBytes int64) {
		// Bound inputs; out-of-range values are dropped, never failed on.
		if capacity < -1024 || capacity > 1024 {
			return
		}
		if totalBytes < 0 || totalBytes > 1_000_000_000 {
			return
		}
		if usedBytes < 0 || usedBytes > totalBytes {
			return
		}
		if minHeadroom < 0 || minHeadroom > 1_000_000_000 {
			return
		}
		if requiredBytes < 0 || requiredBytes > 1_000_000_000 {
			return
		}

		pool := &GPUMemPool{
			totalBytes:  totalBytes,
			usedBytes:   usedBytes,
			backend:     BackendCPU,
			allocations: make(map[unsafe.Pointer]int64),
		}
		avail := int64(0)
		if totalBytes > 0 {
			avail = totalBytes - usedBytes
		}

		// Construction: error exactly when capacity is non-positive, or the
		// pool has a budget and 2*capacity+headroom does not fit in it.
		db, err := NewDoubleBufferWithHeadroom(capacity, pool, minHeadroom)
		if capacity <= 0 {
			require.ErrorContains(t, err, "capacity must be positive")
			require.Nil(t, db)
		} else if totalBytes > 0 && int64(capacity)*2+minHeadroom > avail {
			require.ErrorContains(t, err, "insufficient GPU memory headroom")
			require.Nil(t, db)
		} else {
			require.NoError(t, err)
			require.NotNil(t, db)
			require.Equal(t, capacity, db.Capacity())
		}

		// Nil-pool construction: only the capacity check applies.
		dbNil, errNil := NewDoubleBufferWithHeadroom(capacity, nil, minHeadroom)
		if capacity <= 0 {
			require.ErrorContains(t, errNil, "capacity must be positive")
			require.Nil(t, dbNil)
		} else {
			require.NoError(t, errNil)
			require.NotNil(t, dbNil)
			// With no pool attached every headroom check passes.
			require.NoError(t, dbNil.CheckHeadroom(requiredBytes))
			require.NoError(t, dbNil.CheckHeadroom(requiredBytes))
		}

		if db == nil {
			return
		}

		// CheckHeadroom: deterministic and passes exactly when
		// required+minHeadroom fits in the remaining budget (or no budget set).
		wantOK := totalBytes == 0 || requiredBytes+minHeadroom <= avail
		gotErr := db.CheckHeadroom(requiredBytes)
		gotErr2 := db.CheckHeadroom(requiredBytes)
		if wantOK {
			require.NoError(t, gotErr)
			require.NoError(t, gotErr2)
		} else {
			require.ErrorContains(t, gotErr, "headroom check failed")
			require.EqualError(t, gotErr, gotErr2.Error(), "headroom error must be deterministic")
		}
	})
}
