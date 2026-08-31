package types

import (
	"context"
	"errors"
	"net"
	"testing"
	"bytes"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// grpc_status.go tests
func TestToGRPCStatus(t *testing.T) {
	// nil error
	assert.Nil(t, ToGRPCStatus(nil))

	// already grpc status
	grpcErr := status.Errorf(codes.DataLoss, "data loss")
	assert.Equal(t, grpcErr, ToGRPCStatus(grpcErr))

	// core.ErrNotFound
	errNotFound := core.NewNotFoundError("resource", "123")
	s1 := ToGRPCStatus(errNotFound)
	st1, ok := status.FromError(s1)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st1.Code())

	// core.ErrInvalidArgument
	errInvalid := core.NewInvalidArgumentError("field", "bad")
	s2 := ToGRPCStatus(errInvalid)
	st2, ok := status.FromError(s2)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st2.Code())

	// core.ErrResourceExhausted
	errExhaust := core.NewResourceExhaustedError("memory", "oom")
	s3 := ToGRPCStatus(errExhaust)
	st3, ok := status.FromError(s3)
	assert.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st3.Code())

	// core.ErrUnavailable
	errUnavail := core.NewUnavailableError("service", "down")
	s4 := ToGRPCStatus(errUnavail)
	st4, ok := status.FromError(s4)
	assert.True(t, ok)
	assert.Equal(t, codes.Unavailable, st4.Code())

	// generic error
	errGeneric := errors.New("generic")
	s5 := ToGRPCStatus(errGeneric)
	st5, ok := status.FromError(s5)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st5.Code())
}

// logging_helper.go tests
func TestLogClientAction(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	
	// without peer context
	LogClientAction(context.Background(), logger, nil, "test_action", map[string]any{"key": "val"})
	assert.Contains(t, buf.String(), "test_action")
	assert.Contains(t, buf.String(), "unknown")
	assert.Contains(t, buf.String(), "val")

	// with peer context
	buf.Reset()
	addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1234}
	p := &peer.Peer{Addr: addr}
	ctx := peer.NewContext(context.Background(), p)
	
	LogClientAction(ctx, logger, nil, "test_peer", map[string]any{})
	assert.Contains(t, buf.String(), "test_peer")
	assert.Contains(t, buf.String(), "192.168.1.1:1234")

	// with mesh
	buf.Reset()
	m := &mesh.Gossip{}
	LogClientAction(ctx, logger, m, "test_mesh", map[string]any{})
	assert.Contains(t, buf.String(), "test_mesh")
}

// lazy_metadata.go tests
func TestLazyMetadata(t *testing.T) {
	dataMap := map[string]interface{}{
		"key1": "value1",
		"key2": int64(123),
	}
	encoded, err := core.EncodeMetadata(dataMap)
	require.NoError(t, err)

	lm := NewLazyMetadata(encoded)
	require.NotNil(t, lm)

	// Test GetField (uses ArrowMetadata fast path)
	val, ok := lm.GetField("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	val2, ok2 := lm.GetField("missing")
	assert.False(t, ok2)
	assert.Nil(t, val2)

	// Test Get (forces decode)
	m, err := lm.Get()
	require.NoError(t, err)
	assert.Equal(t, "value1", m["key1"])
	assert.Equal(t, int64(123), m["key2"])

	// Test GetField after Get (should fallback correctly if needed)
	val3, ok3 := lm.GetField("key2")
	assert.True(t, ok3)
	assert.Equal(t, int64(123), val3)
}

func TestLazyMetadata_Invalid(t *testing.T) {
	lm := NewLazyMetadata([]byte("invalid binary data"))
	m, err := lm.Get()
	assert.Error(t, err)
	assert.Nil(t, m)

	val, ok := lm.GetField("any")
	assert.False(t, ok)
	assert.Nil(t, val)
}
