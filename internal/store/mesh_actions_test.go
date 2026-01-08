package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeshActions(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024, 1024*1024, 0)

	// Initialize Mesh
	gcfg := mesh.GossipConfig{
		ID:   "node-1",
		Port: 9999,
	}
	g := mesh.NewGossip(&gcfg)
	s.SetMesh(g)

	meta := NewMetaServer(s)

	t.Run("MeshIdentity", func(t *testing.T) {
		stream := &mockDoActionServer{}
		err := meta.DoAction(&flight.Action{Type: "MeshIdentity"}, stream)
		require.NoError(t, err)
		require.Len(t, stream.results, 1)

		var m mesh.Member
		err = json.Unmarshal(stream.results[0].Body, &m)
		require.NoError(t, err)
		assert.Equal(t, "node-1", m.ID)
	})

	t.Run("MeshStatus", func(t *testing.T) {
		// Mock a few members
		g.UpdateMember(&mesh.Member{ID: "node-2", Addr: "127.0.0.1:8888", Status: mesh.StatusAlive})

		stream := &mockDoActionServer{}
		err := meta.DoAction(&flight.Action{Type: "MeshStatus"}, stream)
		require.NoError(t, err)
		require.Len(t, stream.results, 1)

		var members []mesh.Member
		err = json.Unmarshal(stream.results[0].Body, &members)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(members), 1)
	})

	t.Run("DiscoveryStatus", func(t *testing.T) {
		stream := &mockDoActionServer{}
		err := meta.DoAction(&flight.Action{Type: "DiscoveryStatus"}, stream)
		require.NoError(t, err)
		require.Len(t, stream.results, 1)

		var status map[string]interface{}
		err = json.Unmarshal(stream.results[0].Body, &status)
		require.NoError(t, err)
		assert.Contains(t, status, "provider")
	})

	t.Run("ClusterStatus", func(t *testing.T) {
		stream := &mockDoActionServer{}
		err := meta.DoAction(&flight.Action{Type: "cluster-status"}, stream)
		require.NoError(t, err)
		require.Len(t, stream.results, 1)

		var status map[string]interface{}
		err = json.Unmarshal(stream.results[0].Body, &status)
		require.NoError(t, err)
		assert.Contains(t, status, "self")
		assert.Contains(t, status, "members")
		assert.Contains(t, status, "count")
	})
}

type mockDoActionServer struct {
	flight.FlightService_DoActionServer
	results []*flight.Result
}

func (m *mockDoActionServer) Send(res *flight.Result) error {
	m.results = append(m.results, res)
	return nil
}

func (m *mockDoActionServer) Context() context.Context {
	return context.Background()
}
