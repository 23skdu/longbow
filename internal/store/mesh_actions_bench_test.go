package store

import (
	"fmt"
	"testing"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func BenchmarkMeshStatus(b *testing.B) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<30, 1<<30, 0)

	gcfg := mesh.GossipConfig{
		ID:   "node-master",
		Port: 9999,
	}
	g := mesh.NewGossip(gcfg)
	s.SetMesh(g)

	meta := NewMetaServer(s)

	// Pre-fill members
	for i := 0; i < 1000; i++ {
		g.UpdateMember(&mesh.Member{
			ID:     fmt.Sprintf("node-%d", i),
			Addr:   fmt.Sprintf("127.0.0.1:%d", 10000+i),
			Status: mesh.StatusAlive,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := &mockDoActionServer{}
		_ = meta.DoAction(&flight.Action{Type: "MeshStatus"}, stream)
	}
}
