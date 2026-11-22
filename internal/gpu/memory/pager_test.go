//go:build gpu && linux

package memory

import (
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPager(t *testing.T) *GPUPager {
	t.Helper()
	pool, err := NewGPUMemPool(types.BackendCPU, 0)
	require.NoError(t, err)
	return NewGPUPager(pool, 64*1024, 4096) // 16 pages of 4KB
}

// TestNewGPUPager runs the test new gpu pager operation.
func TestNewGPUPager(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()
	assert.Equal(t, 0, p.TotalPages())
	assert.Equal(t, int64(0), p.VRAMUsage())
}

// TestGPUPager_AllocPromote runs the test gpu pager_alloc promote operation.
func TestGPUPager_AllocPromote(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	pi, err := p.Alloc(1)
	require.NoError(t, err)
	require.NotNil(t, pi)
	assert.Equal(t, PageID(1), pi.id)
	assert.Equal(t, 1, p.TotalPages())

	err = p.Promote(pi)
	require.NoError(t, err)
	assert.Equal(t, 1, p.ResidentPages())
	assert.NotNil(t, p.GetGPUAddr(pi))
}

// TestGPUPager_DoubleAllocFails runs the test gpu pager_double alloc fails operation.
func TestGPUPager_DoubleAllocFails(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	_, err := p.Alloc(1)
	require.NoError(t, err)

	_, err = p.Alloc(1)
	assert.Error(t, err)
}

// TestGPUPager_Free runs the test gpu pager_free operation.
func TestGPUPager_Free(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	pi, err := p.Alloc(1)
	require.NoError(t, err)
	require.NoError(t, p.Promote(pi))
	assert.Equal(t, 1, p.ResidentPages())

	require.NoError(t, p.Free(1))
	assert.Equal(t, 0, p.TotalPages())
	assert.Equal(t, 0, p.ResidentPages())
}

// TestGPUPager_LRUEviction runs the test gpu pager_lru eviction operation.
func TestGPUPager_LRUEviction(t *testing.T) {
	pool, err := NewGPUMemPool(types.BackendCPU, 0)
	require.NoError(t, err)

	// Only 2 pages fit in VRAM
	p := NewGPUPager(pool, 8192, 4096)
	defer p.Close()

	p1, err := p.Alloc(1)
	require.NoError(t, err)
	p2, err := p.Alloc(2)
	require.NoError(t, err)
	p3, err := p.Alloc(3)
	require.NoError(t, err)

	// Promote first two - should both fit
	require.NoError(t, p.Promote(p1))
	require.NoError(t, p.Promote(p2))
	assert.Equal(t, 2, p.ResidentPages())

	// Promote p3 - should evict LRU (p1)
	require.NoError(t, p.Promote(p3))
	assert.Equal(t, 2, p.ResidentPages())

	// p1 should now be evicted
	evicts, restores, fails := p.Stats()
	assert.Equal(t, int64(1), evicts)
	assert.Equal(t, int64(0), restores)
	assert.Equal(t, int64(0), fails)
}

// TestGPUPager_Demote runs the test gpu pager_demote operation.
func TestGPUPager_Demote(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	pi, err := p.Alloc(1)
	require.NoError(t, err)
	require.NoError(t, p.Promote(pi))
	assert.Equal(t, 1, p.ResidentPages())

	require.NoError(t, p.Demote(pi))
	assert.Equal(t, 0, p.ResidentPages())
}

// TestGPUPager_MarkDirty runs the test gpu pager_mark dirty operation.
func TestGPUPager_MarkDirty(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	pi, err := p.Alloc(1)
	require.NoError(t, err)
	require.NoError(t, p.Promote(pi))

	p.MarkDirty(pi)
	require.NoError(t, p.Demote(pi))
	assert.Equal(t, 0, p.ResidentPages())
}

// TestGPUPager_Restore runs the test gpu pager_restore operation.
func TestGPUPager_Restore(t *testing.T) {
	pool, err := NewGPUMemPool(types.BackendCPU, 0)
	require.NoError(t, err)

	p := NewGPUPager(pool, 8192, 4096)
	defer p.Close()

	p1, err := p.Alloc(1)
	require.NoError(t, err)
	p2, err := p.Alloc(2)
	require.NoError(t, err)

	require.NoError(t, p.Promote(p1))
	require.NoError(t, p.Promote(p2))

	// Force eviction of p1 by promoting p1 again (should be NOP since already resident)
	// Then demote p2 to force p1's data to be written back
	require.NoError(t, p.Demote(p2))
	assert.Equal(t, 1, p.ResidentPages())

	// Restore p2
	require.NoError(t, p.Promote(p2))
	assert.Equal(t, 2, p.ResidentPages())

	_, restores, _ := p.Stats()
	assert.Equal(t, int64(1), restores)
}

// TestGPUPager_Access runs the test gpu pager_access operation.
func TestGPUPager_Access(t *testing.T) {
	pool, err := NewGPUMemPool(types.BackendCPU, 0)
	require.NoError(t, err)

	p := NewGPUPager(pool, 8192, 4096)
	defer p.Close()

	p1, err := p.Alloc(1)
	require.NoError(t, err)
	p2, err := p.Alloc(2)
	require.NoError(t, err)
	p3, err := p.Alloc(3)
	require.NoError(t, err)

	require.NoError(t, p.Promote(p1))
	require.NoError(t, p.Promote(p2))

	// Access p1 to make it MRU
	p.Access(p1)

	// Promote p3 - should evict p2 (now LRU), not p1
	require.NoError(t, p.Promote(p3))

	// p1 should still be resident (was accessed recently)
	s1 := PageState(p1.state.Load())
	assert.Equal(t, PageResident, s1, "p1 should still be resident after access")

	evicts, _, _ := p.Stats()
	assert.Equal(t, int64(1), evicts)
}

// TestGPUPager_Close runs the test gpu pager_close operation.
func TestGPUPager_Close(t *testing.T) {
	p := newTestPager(t)
	defer p.Close()

	pi, err := p.Alloc(1)
	require.NoError(t, err)
	require.NoError(t, p.Promote(pi))

	require.NoError(t, p.Close())
	assert.Equal(t, 0, p.TotalPages())
}

// TestGPUPager_AllocAfterClose runs the test gpu pager_alloc after close operation.
func TestGPUPager_AllocAfterClose(t *testing.T) {
	p := newTestPager(t)
	p.Close()

	_, err := p.Alloc(1)
	assert.Error(t, err)
}
