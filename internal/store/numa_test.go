package store

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectNUMATopology(t *testing.T) {
	topo, err := DetectNUMATopology()
	require.NoError(t, err)
	require.NotNil(t, topo)

	// Should always return at least one node
	assert.GreaterOrEqual(t, topo.NumNodes, 1)

	// Should have CPU information
	assert.Len(t, topo.CPUs, topo.NumNodes)

	t.Logf("Detected NUMA topology: %s", topo.String())
}

func TestNUMATopologyString(t *testing.T) {
	topo := &NUMATopology{
		NumNodes: 2,
		CPUs: [][]int{
			{0, 1, 2, 3},
			{4, 5, 6, 7},
		},
	}

	str := topo.String()

	// On Linux, should show multi-node info
	// On other platforms, stub returns single-node message
	if runtime.GOOS == "linux" {
		assert.Contains(t, str, "2 NUMA nodes")
		assert.Contains(t, str, "Node 0")
		assert.Contains(t, str, "Node 1")
	} else {
		// Non-Linux platforms use stub
		assert.Contains(t, str, "Single NUMA node")
	}
}
