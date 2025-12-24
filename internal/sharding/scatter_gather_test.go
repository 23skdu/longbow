package sharding

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"go.uber.org/zap"
)

func TestScatterGather_Scatter(t *testing.T) {
	// Setup
	rm := NewRingManager("node-1", zap.NewNop())
	rm.NotifyJoin(&mesh.Member{ID: "node-1", Addr: "127.0.0.1:9090"})
	rm.NotifyJoin(&mesh.Member{ID: "node-2", Addr: "127.0.0.1:9091"})
	rm.NotifyJoin(&mesh.Member{ID: "node-3", Addr: "127.0.0.1:9092"})

	sg := NewScatterGather(rm, nil, zap.NewNop()) // Forwarder nil as we won't use it in mock

	// Mock function that returns node ID
	fn := func(ctx context.Context, nodeID string) (interface{}, error) {
		// Simulate network latency
		time.Sleep(10 * time.Millisecond)
		return "response-from-" + nodeID, nil
	}

	start := time.Now()
	results, err := sg.Scatter(context.Background(), fn)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Scatter failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Verify parallel execution (duration should be ~10ms, not 30ms)
	if duration > 25*time.Millisecond {
		t.Errorf("Execution took too long for parallel scatter: %v", duration)
	}

	seen := make(map[string]bool)
	for _, res := range results {
		seen[res.NodeID] = true
		expected := "response-from-" + res.NodeID
		if res.Data != expected {
			t.Errorf("Expected data %s, got %s", expected, res.Data)
		}
	}

	if len(seen) != 3 {
		t.Error("Did not get results from all nodes")
	}
}

func BenchmarkScatterGather(b *testing.B) {
	rm := NewRingManager("node-1", zap.NewNop())
	// create 100 fake nodes
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("node-%d", i)
		rm.NotifyJoin(&mesh.Member{ID: id, Addr: "1.1.1.1"})
	}

	sg := NewScatterGather(rm, nil, zap.NewNop())
	ctx := context.Background()
	fn := func(ctx context.Context, nodeID string) (interface{}, error) {
		return 1, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sg.Scatter(ctx, fn)
	}
}
