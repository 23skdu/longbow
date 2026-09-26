//go:build !gpu || !linux || !cgo

package cuda

import (
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
)

func TestCUDAStub(t *testing.T) {
	cfg := types.GPUConfig{
		DeviceID:  0,
		Dimension: 128,
		Enabled:   true,
	}

	idx, err := NewCUDAIndexImpl(cfg)
	if err == nil || idx != nil {
		t.Errorf("expected error from NewCUDAIndexImpl on stub, got %v", idx)
	}

	ptr, err := HostAlloc(1024)
	if err == nil || ptr != nil {
		t.Errorf("expected error from HostAlloc on stub, got %v", ptr)
	}

	if err := FreeHost(nil); err != nil {
		t.Errorf("FreeHost on stub failed: %v", err)
	}

	if err := MemcpyAsync(nil, nil, 1024, MemcpyHostToDevice, nil); err == nil {
		t.Errorf("expected error from MemcpyAsync on stub")
	}

	if err := StreamSynchronize(nil); err == nil {
		t.Errorf("expected error from StreamSynchronize on stub")
	}

	pool := NewPinnedHostPool()
	if pool == nil {
		t.Fatal("NewPinnedHostPool returned nil")
	}

	if p, err := pool.Get(1024); err == nil || p != nil {
		t.Errorf("expected error from pool.Get on stub, got %v", p)
	}

	pool.Put(nil, 1024)

	if err := pool.Close(); err != nil {
		t.Errorf("pool.Close on stub failed: %v", err)
	}
}
