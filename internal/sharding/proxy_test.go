package sharding

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestPartitionProxyInterceptor_Local(t *testing.T) {
	logger := zap.NewNop()
	rm := NewRingManager("local-node", logger)
	rm.ring.AddNode("local-node") // Ensure local node is in ring

	// Key hashing to ensure it lands on local-node
	// Since we only have one node, all keys land on it
	key := "some-key"

	interceptor := PartitionProxyInterceptor(rm, &RequestForwarder{})
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "success", nil
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-longbow-key", key))
	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)

	assert.NoError(t, err)
	assert.Equal(t, "success", resp)
}

func TestPartitionProxyInterceptor_Remote(t *testing.T) {
	logger := zap.NewNop()
	rm := NewRingManager("local-node", logger)
	rm.ring.AddNode("remote-node")
	// Make sure local-node is NOT in the ring or at least ensure the key maps to remote-node
	// By default NewRingManager adds NO nodes initially besides what we add.
	// So consistent hash has only "remote-node". All keys go there.

	key := "some-key"
	interceptor := PartitionProxyInterceptor(rm, &RequestForwarder{})
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "should-not-be-called", nil
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-longbow-key", key))
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)

	// Since RequestForwarder is a stub returning error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WRONG_NODE")
}
