package store

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNUMAConfig(t *testing.T) {
	cfg := NewNUMAConfig()
	require.NotNil(t, cfg)
	assert.False(t, cfg.Enabled)
	assert.Equal(t, AutoDetect, cfg.NodeSelection)
	assert.Empty(t, cfg.PreferredNodes)
}

func TestNUMAConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     NUMAConfig
		wantErr bool
	}{
		{"disabled", NUMAConfig{Enabled: false}, false},
		{"auto-detect", NUMAConfig{Enabled: true, NodeSelection: AutoDetect}, false},
		{"round-robin", NUMAConfig{Enabled: true, NodeSelection: RoundRobin}, false},
		{"prefer-nodes valid", NUMAConfig{Enabled: true, NodeSelection: PreferNodes, PreferredNodes: []int{0}}, false},
		{"prefer-nodes empty", NUMAConfig{Enabled: true, NodeSelection: PreferNodes}, true},
		{"invalid mode", NUMAConfig{Enabled: true, NodeSelection: NodeSelectionMode(99)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNUMATopology_Detect(t *testing.T) {
	topo, err := DetectNUMATopology()
	if err != nil {
		t.Skipf("NUMA not available: %v", err)
	}
	require.NotNil(t, topo)
	assert.GreaterOrEqual(t, topo.NumNodes(), 1)
	assert.GreaterOrEqual(t, topo.NumCPUs(), 1)
}

func TestNUMATopology_GetNodeCPUs(t *testing.T) {
	topo, err := DetectNUMATopology()
	if err != nil {
		t.Skipf("NUMA not available: %v", err)
	}
	for node := 0; node < topo.NumNodes(); node++ {
		cpus := topo.GetNodeCPUs(node)
		assert.NotEmpty(t, cpus, "node %d should have CPUs", node)
	}
}

func TestIsNUMAAvailable(t *testing.T) {
	available := IsNUMAAvailable()
	t.Logf("NUMA available: %v", available)
}

func TestNewNUMAAllocator(t *testing.T) {
	cfg := NUMAConfig{Enabled: true, NodeSelection: AutoDetect}
	alloc, err := NewNUMAAllocator(cfg)
	if err != nil {
		t.Skipf("Could not create NUMA allocator: %v", err)
	}
	require.NotNil(t, alloc)
	assert.True(t, alloc.IsEnabled())
}

func TestNUMAAllocator_Disabled(t *testing.T) {
	cfg := NUMAConfig{Enabled: false}
	alloc, err := NewNUMAAllocator(cfg)
	require.NoError(t, err)
	require.NotNil(t, alloc)
	assert.False(t, alloc.IsEnabled())
	buf := alloc.AllocBytes(1024)
	assert.Equal(t, 1024, len(buf))
}

func TestNUMAAllocator_AllocBytes(t *testing.T) {
	cfg := NUMAConfig{Enabled: false}
	alloc, _ := NewNUMAAllocator(cfg)
	buf := alloc.AllocBytes(4096)
	require.NotNil(t, buf)
	assert.Equal(t, 4096, len(buf))
	for i := range buf {
		buf[i] = byte(i % 256)
	}
}

func TestNUMAAllocator_AllocFloat32Slice(t *testing.T) {
	cfg := NUMAConfig{Enabled: false}
	alloc, _ := NewNUMAAllocator(cfg)
	floats := alloc.AllocFloat32Slice(256)
	require.NotNil(t, floats)
	assert.Equal(t, 256, len(floats))
	for i := range floats {
		floats[i] = float32(i)
	}
	assert.Equal(t, float32(100), floats[100])
}

func TestNUMAAllocator_AllocOnNode(t *testing.T) {
	cfg := NUMAConfig{Enabled: true, NodeSelection: AutoDetect}
	alloc, err := NewNUMAAllocator(cfg)
	if err != nil {
		t.Skipf("NUMA not available: %v", err)
	}
	buf, err := alloc.AllocBytesOnNode(4096, 0)
	if err != nil {
		t.Skipf("Could not allocate on node 0: %v", err)
	}
	assert.Equal(t, 4096, len(buf))
}

func TestNUMAGoroutineBinding(t *testing.T) {
	if !IsNUMAAvailable() {
		t.Skip("NUMA not available")
	}
	cfg := NUMAConfig{Enabled: true, NodeSelection: AutoDetect}
	alloc, err := NewNUMAAllocator(cfg)
	if err != nil {
		t.Skipf("Could not create allocator: %v", err)
	}
	err = alloc.BindGoroutineToNode(0)
	if err != nil {
		t.Skipf("Could not bind goroutine: %v", err)
	}
	node := alloc.GetCurrentNode()
	assert.GreaterOrEqual(t, node, 0)
}

func TestNUMAAllocator_RoundRobin(t *testing.T) {
	cfg := NUMAConfig{Enabled: true, NodeSelection: RoundRobin}
	alloc, err := NewNUMAAllocator(cfg)
	if err != nil {
		t.Skipf("NUMA not available: %v", err)
	}
	nodes := make([]int, 10)
	for i := 0; i < 10; i++ {
		nodes[i] = alloc.NextNode()
	}
	if alloc.NumNodes() > 1 {
		assert.NotEqual(t, nodes[0], nodes[1])
	}
}

func TestNUMAAllocator_PreferredNodes(t *testing.T) {
	cfg := NUMAConfig{Enabled: true, NodeSelection: PreferNodes, PreferredNodes: []int{0}}
	alloc, err := NewNUMAAllocator(cfg)
	if err != nil {
		t.Skipf("NUMA not available: %v", err)
	}
	for i := 0; i < 5; i++ {
		node := alloc.NextNode()
		assert.Equal(t, 0, node)
	}
}

func TestNUMAAllocator_GetStats(t *testing.T) {
	cfg := NUMAConfig{Enabled: false}
	alloc, _ := NewNUMAAllocator(cfg)
	_ = alloc.AllocBytes(1024)
	_ = alloc.AllocBytes(2048)
	stats := alloc.GetStats()
	assert.GreaterOrEqual(t, stats.TotalAllocations, int64(2))
	assert.GreaterOrEqual(t, stats.TotalBytes, int64(3072))
}

func TestGetCurrentCPU(t *testing.T) {
	cpu := GetCurrentCPU()
	assert.GreaterOrEqual(t, cpu, -1)
	assert.Less(t, cpu, runtime.NumCPU()+1)
}

func BenchmarkNUMAAllocator_AllocBytes(b *testing.B) {
	cfg := NUMAConfig{Enabled: false}
	alloc, _ := NewNUMAAllocator(cfg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = alloc.AllocBytes(4096)
	}
}

func BenchmarkStandardAlloc_Bytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]byte, 4096)
	}
}

func BenchmarkNUMAAllocator_Float32(b *testing.B) {
	cfg := NUMAConfig{Enabled: false}
	alloc, _ := NewNUMAAllocator(cfg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = alloc.AllocFloat32Slice(256)
	}
}
