package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockRemoteStorage(t *testing.T) {
	mock := NewMockRemoteStorage("s3")
	ctx := context.Background()

	// 1. Put
	data := []byte("hello world")
	err := mock.Put(ctx, "test.txt", bytes.NewReader(data))
	require.NoError(t, err)

	// 2. Exists
	exists, err := mock.Exists(ctx, "test.txt")
	require.NoError(t, err)
	require.True(t, exists)

	// 3. Get
	rc, err := mock.Get(ctx, "test.txt")
	require.NoError(t, err)
	defer rc.Close()
	
	downloaded, _ := bytes.NewBuffer(nil).ReadFrom(rc)
	require.Equal(t, int64(len(data)), downloaded)

	// 4. Delete
	err = mock.Delete(ctx, "test.txt")
	require.NoError(t, err)

	exists, _ = mock.Exists(ctx, "test.txt")
	require.False(t, exists)
}
