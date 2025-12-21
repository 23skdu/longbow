package sharding

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/23skdu/longbow/internal/mesh"
)

func TestPartitionProxyInterceptor_Local(t *testing.T) {
	logger := zap.NewNop()
	rm := NewRingManager("local-node", logger)
	rm.NotifyJoin(&mesh.Member{ID: "local-node", Addr: "localhost:1234", Status: mesh.StatusAlive})

	// Key hashing to ensure it lands on local-node
	// Since we only have one node, all keys land on it
	key := "some-key"

	forwarder := NewRequestForwarder(DefaultForwarderConfig(), rm)
	interceptor := PartitionProxyInterceptor(rm, forwarder)
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
	rm.NotifyJoin(&mesh.Member{ID: "remote-node", Addr: "remotehost:5678", Status: mesh.StatusAlive})
	// Make sure local-node is NOT in the ring or at least ensure the key maps to remote-node
	// By default NewRingManager adds NO nodes initially besides what we add.
	// So consistent hash has only "remote-node". All keys go there.

	key := "some-key"
	forwarder := NewRequestForwarder(DefaultForwarderConfig(), rm)
	interceptor := PartitionProxyInterceptor(rm, forwarder)
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "should-not-be-called", nil
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-longbow-key", key))
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)

	// Since RequestForwarder tries to dial a non-existent host, we expect a dial error
	assert.Error(t, err)
	// We expect the error to come from the forwarder's GetConn
	assert.Contains(t, err.Error(), "forwarder: get conn")
}
