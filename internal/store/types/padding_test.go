package types

import (
	"testing"
	"unsafe"
)

func TestPaddedMutexAlignment(t *testing.T) {
	var m PaddedMutex
	size := unsafe.Sizeof(m)
	if size != 64 {
		t.Errorf("expected PaddedMutex size to be 64, got %d", size)
	}
}
