//go:build cgo

package main

import (
	"math"
	"reflect"
	"testing"
	"unsafe"
)

// referenceADBCVersionSupported is the documented contract of
// AdbcLongbowAdbcInit: the driver negotiates the 1.x ADBC family
// (ADBC_VERSION_1_0_0 = 1000000 .. 1001999), plus the unversioned
// "0" and the pre-1.0 sentinel "1".
func referenceADBCVersionSupported(v int) bool {
	return v == 0 || v == 1 || (v >= 1000000 && v < 1002000)
}

// FuzzAdbcInitVersion fuzzes ADBC version negotiation in AdbcLongbowAdbcInit.
// Invariant: the init entry point reports OK exactly for the versions
// isSupportedADBCVersion (and the documented contract) accept, and reports
// INVALID_ARGUMENT for everything else - consistently across repeated calls.
//
// The driver table argument points at a scratch buffer large enough for
// struct AdbcDriver, so the supported path can fill the table safely; the
// unsupported path must never touch it.
func FuzzAdbcInitVersion(f *testing.F) {
	seeds := []int{
		0,       // unversioned default: supported
		1,       // pre-1.0 sentinel: supported
		1000000, // ADBC_VERSION_1_0_0
		1001000, // ADBC_VERSION_1_1_0
		1001999, // last supported 1.x patch
		999,     // below the 1.x band
		1002000, // first unsupported 1.x-band exit
		-1,      // negative
	}
	for _, v := range seeds {
		f.Add(v)
	}

	// Invoked through reflect so this test file needs no cgo import of its own.
	fn := reflect.ValueOf(AdbcLongbowAdbcInit)
	cIntType := fn.Type().In(0)
	cErrType := fn.Type().In(2)

	f.Fuzz(func(t *testing.T, version int) {
		// C.int is 32 bits; anything wider is outside the wire domain.
		if version < math.MinInt32 || version > math.MaxInt32 {
			return
		}

		wantSupported := referenceADBCVersionSupported(version)
		if got := isSupportedADBCVersion(version); got != wantSupported {
			t.Fatalf("isSupportedADBCVersion(%d) = %v, want %v", version, got, wantSupported)
		}

		// Scratch buffer for struct AdbcDriver (well under 4096 bytes of
		// function pointers); the init path only writes into it.
		buf := make([]byte, 4096)
		args := []reflect.Value{
			reflect.ValueOf(int64(version)).Convert(cIntType),
			reflect.ValueOf(unsafe.Pointer(&buf[0])),
			reflect.Zero(cErrType),
		}

		call := func() int64 {
			out := fn.Call(args)
			if len(out) != 1 {
				t.Fatalf("AdbcLongbowAdbcInit returned %d values, want 1", len(out))
			}
			return out[0].Int()
		}

		rc := call()
		if wantSupported {
			if rc != int64(adbcStatusOK) {
				t.Fatalf("AdbcLongbowAdbcInit(%d) = %d, want OK(%d) for supported version", version, rc, adbcStatusOK)
			}
		} else if rc != int64(adbcStatusInvalidArgument) {
			t.Fatalf("AdbcLongbowAdbcInit(%d) = %d, want INVALID_ARGUMENT(%d) for unsupported version",
				version, rc, adbcStatusInvalidArgument)
		}

		// Negotiation is deterministic.
		if again := call(); again != rc {
			t.Fatalf("AdbcLongbowAdbcInit(%d) not deterministic: first=%d second=%d", version, rc, again)
		}
	})
}
