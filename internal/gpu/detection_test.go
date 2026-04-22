package gpu

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/stretchr/testify/require"
)

func TestDetectTPUs_Mock(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("detectTPUs is Linux-only")
	}

	// Mock sysfs
	tmpDir := t.TempDir()
	accelDir := filepath.Join(tmpDir, "sys/class/accel")
	err := os.MkdirAll(accelDir, 0755)
	require.NoError(t, err)

	devDir := filepath.Join(accelDir, "accel0/device")
	err = os.MkdirAll(devDir, 0755)
	require.NoError(t, err)

	// Write mock vendor ID (Google)
	err = os.WriteFile(filepath.Join(devDir, "vendor"), []byte("0x1ae0\n"), 0644)
	require.NoError(t, err)

	// Write mock device ID (Ironwood)
	err = os.WriteFile(filepath.Join(devDir, "device"), []byte("0x0063\n"), 0644)
	require.NoError(t, err)

	// Write mock NUMA node
	err = os.WriteFile(filepath.Join(devDir, "numa_node"), []byte("1\n"), 0644)
	require.NoError(t, err)

	// Verify detection logic
	gpus := detectTPUsWithRoot(accelDir)
	require.Len(t, gpus, 1)
	require.Equal(t, types.BackendTPU, gpus[0].Backend)
	require.Equal(t, "Google TPU v7x (Ironwood)", gpus[0].Name)
	require.Equal(t, 1, gpus[0].ComputeMajor) // NUMA node
}

func TestGPUBackend_String(t *testing.T) {
	require.Equal(t, "TPU", types.BackendTPU.String())
}
