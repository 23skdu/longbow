package memory

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNUMATopology_GetNodeForCPU(t *testing.T) {
	topo := &NUMATopology{
		NumNodes: 2,
		CPUs: [][]int{
			{0, 1, 2, 3},
			{4, 5, 6, 7},
		},
	}

	assert.Equal(t, 0, topo.GetNodeForCPU(0))
	assert.Equal(t, 0, topo.GetNodeForCPU(3))

	if runtime.GOOS == "linux" {
		assert.Equal(t, 1, topo.GetNodeForCPU(4))
		assert.Equal(t, 1, topo.GetNodeForCPU(7))
		assert.Equal(t, -1, topo.GetNodeForCPU(8))
	} else {
		// Stub returns 0 for any positive CPU
		assert.Equal(t, 0, topo.GetNodeForCPU(4))
		assert.Equal(t, 0, topo.GetNodeForCPU(8))
	}
	assert.Equal(t, -1, topo.GetNodeForCPU(-1))
}
