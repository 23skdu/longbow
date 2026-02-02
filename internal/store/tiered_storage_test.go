package store

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTieredStorage_OffloadAndFetch(t *testing.T) {
	path := "test_tiered.dvs"
	defer os.Remove(path)

	dim := 128
	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	remote := storage.NewMockRemoteStorage()
	dvs.SetTieredConfig(remote, 10) // 10MB cache

	// 1. Add some vectors
	vectors := make([][]float32, 10)
	for i := 0; i < 10; i++ {
		v := make([]float32, dim)
		v[0] = float32(i)
		vectors[i] = v
	}

	n, err := dvs.BatchAppend(vectors)
	require.NoError(t, err)
	assert.Equal(t, 10, n)

	// 2. Offload block 0
	ctx := context.Background()
	err = dvs.OffloadBlock(ctx, 0)
	require.NoError(t, err)

	// Verify it's in remote
	exists, _ := remote.Exists(ctx, fmt.Sprintf("blocks/%s/%d", path, 0))
	assert.True(t, exists)

	// 3. Fetch vectors (transparently from remote)
	indices := []int{0, 5, 9}
	results, err := dvs.GetBatch(indices)
	require.NoError(t, err)
	require.Equal(t, len(indices), len(results))

	for i, idx := range indices {
		assert.Equal(t, float32(idx), results[i][0])
	}
}

func TestTieredStorage_EnforcePolicy(t *testing.T) {
	path := "test_policy.dvs"
	defer os.Remove(path)

	dim := 128
	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	remote := storage.NewMockRemoteStorage()
	dvs.SetTieredConfig(remote, 10)

	// Add vector
	v := make([]float32, dim)
	v[0] = 1.23
	_, _ = dvs.BatchAppend([][]float32{v})

	// Enforce policy with 0 age (all blocks qualify)
	ctx := context.Background()
	n, err := dvs.EnforcePolicy(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Verify block 0 is warm
	results, err := dvs.GetBatch([]int{0})
	require.NoError(t, err)
	assert.Equal(t, float32(1.23), results[0][0])
}
