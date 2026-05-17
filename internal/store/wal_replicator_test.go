package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockPeerPool struct {
	mock.Mock
}

func (m *MockPeerPool) DoAction(ctx context.Context, host string, action *flight.Action) error {
	args := m.Called(ctx, host, action)
	return args.Error(0)
}

type MockMesh struct {
	mock.Mock
}

func (m *MockMesh) GetMembers() []mesh.Member {
	args := m.Called()
	return args.Get(0).([]mesh.Member)
}

func TestFlightWALReplicator_Replicate_QuorumSuccess(t *testing.T) {
	pool := new(MockPeerPool)
	msh := new(MockMesh)

	members := []mesh.Member{
		{ID: "node2", GRPCAddr: "127.0.0.1:3001"},
		{ID: "node3", GRPCAddr: "127.0.0.1:3002"},
	}
	msh.On("GetMembers").Return(members)

	// Total nodes = 3 (self + 2 remote). Required remote acks = floor(3/2) = 1.
	// We only need 1 remote ack to succeed.
	
	data := []byte("test-wal-data")
	
	pool.On("DoAction", mock.Anything, "127.0.0.1:3001", mock.MatchedBy(func(a *flight.Action) bool {
		return a.Type == "ReplicateWAL"
	})).Return(nil)
	
	// Node 3 fails, but we already have 1 ack from node 2.
	pool.On("DoAction", mock.Anything, "127.0.0.1:3002", mock.Anything).Return(fmt.Errorf("failed")).Maybe()

	replicator := NewFlightWALReplicator(pool, msh)
	err := replicator.Replicate(context.Background(), data)

	assert.NoError(t, err)
	pool.AssertExpectations(t)
}

func TestFlightWALReplicator_Replicate_QuorumFailure(t *testing.T) {
	pool := new(MockPeerPool)
	msh := new(MockMesh)

	members := []mesh.Member{
		{ID: "node2", GRPCAddr: "127.0.0.1:3001"},
		{ID: "node3", GRPCAddr: "127.0.0.1:3002"},
	}
	msh.On("GetMembers").Return(members)

	// Total nodes = 3. Required remote acks = 1.
	// Both fail.
	
	data := []byte("test-wal-data")
	
	pool.On("DoAction", mock.Anything, "127.0.0.1:3001", mock.Anything).Return(fmt.Errorf("error1"))
	pool.On("DoAction", mock.Anything, "127.0.0.1:3002", mock.Anything).Return(fmt.Errorf("error2"))

	replicator := NewFlightWALReplicator(pool, msh)
	replicator.timeout = 100 * time.Millisecond
	
	err := replicator.Replicate(context.Background(), data)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to reach WAL replication quorum")
}
