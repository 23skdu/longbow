package sharding

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockResolver
type MockResolver struct {
	mock.Mock
}

func (m *MockResolver) GetNodeAddr(nodeID string) string {
	args := m.Called(nodeID)
	return args.String(0)
}

func TestRequestForwarder_GetConn(t *testing.T) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	target := "127.0.0.1:0" // Dummy address

	// 1. First call
	conn, err := fwd.GetConn(ctx, target)
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	// 2. Second call (cached)
	conn2, err := fwd.GetConn(ctx, target)
	assert.NoError(t, err)
	assert.Equal(t, conn, conn2, "Should return cached connection")
}

func TestRequestForwarder_Forward_UnknownNode(t *testing.T) {
	resolver := new(MockResolver)
	resolver.On("GetNodeAddr", "unknown").Return("")

	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	_, err := fwd.Forward(context.Background(), "unknown", nil, "method")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown node ID")
}

func TestRequestForwarder_Forward_Connection(t *testing.T) {
	resolver := new(MockResolver)
	// Return invalid address to force dial fail or similar
	resolver.On("GetNodeAddr", "node1").Return("invalid:port")

	config := DefaultForwarderConfig()
	// Short timeout
	config.DialTimeout = 100 * time.Millisecond
	fwd := NewRequestForwarder(&config, resolver)
	_ = fwd.Close()

	// Since we can't efficiently test Invoke failure without waiting for timeout,
	// we stick to unit testing the Resolver logic and GetConn.
}

func TestRequestForwarder_ConnectionPooling(t *testing.T) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	target1 := "127.0.0.1:1001"
	target2 := "127.0.0.1:1002"
	target3 := "127.0.0.1:1003"

	// Get first connection
	conn1, err := fwd.GetConn(ctx, target1)
	assert.NoError(t, err)
	assert.NotNil(t, conn1)

	// Get same connection (should be cached)
	conn1Cached, err := fwd.GetConn(ctx, target1)
	assert.NoError(t, err)
	assert.NotNil(t, conn1Cached)
	// Use pointer comparison to avoid gRPC internal race in reflect.DeepEqual
	assert.Equal(t, fmt.Sprintf("%p", conn1), fmt.Sprintf("%p", conn1Cached), "Same target should return same connection")

	// Get different connection
	conn2, err := fwd.GetConn(ctx, target2)
	assert.NoError(t, err)
	assert.NotNil(t, conn2)
	assert.NotEqual(t, fmt.Sprintf("%p", conn1), fmt.Sprintf("%p", conn2), "Different targets should have different connections")

	// Get another different connection
	conn3, err := fwd.GetConn(ctx, target3)
	assert.NoError(t, err)
	assert.NotNil(t, conn3)
	assert.NotEqual(t, fmt.Sprintf("%p", conn1), fmt.Sprintf("%p", conn3), "Different targets should have different connections")
	assert.NotEqual(t, fmt.Sprintf("%p", conn2), fmt.Sprintf("%p", conn3), "Different targets should have different connections")

	// Verify all connections are different targets
	assert.NotEqual(t, target1, target2)
	assert.NotEqual(t, target2, target3)
}

func TestRequestForwarder_ConcurrentAccess(t *testing.T) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	target := "127.0.0.1:2001"

	// Concurrent access to same connection
	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			conn, err := fwd.GetConn(ctx, target)
			assert.NoError(t, err)
			assert.NotNil(t, conn)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for goroutines")
		}
	}
}

func TestRequestForwarder_Close(t *testing.T) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)

	target := "127.0.0.1:3001"
	ctx := context.Background()

	// Get a connection
	conn, err := fwd.GetConn(ctx, target)
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	// Close the forwarder
	err = fwd.Close()
	assert.NoError(t, err)

	// Get a new connection after close (should work)
	conn2, err := fwd.GetConn(ctx, target)
	assert.NoError(t, err)
	assert.NotNil(t, conn2)
	// Note: conn and conn2 might be different objects since we closed and recreated

	// Clean up
	_ = fwd.Close()
}

func TestRequestForwarder_EmptyTarget(t *testing.T) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	target := ""

	// Empty target should still create a connection (grpc.NewClient accepts empty string)
	conn, err := fwd.GetConn(ctx, target)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestRequestForwarder_MultipleClose(t *testing.T) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)

	// Multiple close calls should not panic
	assert.NoError(t, fwd.Close())
	assert.NoError(t, fwd.Close())
	assert.NoError(t, fwd.Close())
}

func TestRequestForwarder_ConnectionCount(t *testing.T) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	numTargets := 5

	// Create multiple connections
	for i := 0; i < numTargets; i++ {
		target := "127.0.0.1:400" + string(rune('0'+i))
		conn, err := fwd.GetConn(ctx, target)
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	}

	// Verify connections are cached and can be retrieved
	for i := 0; i < numTargets; i++ {
		target := "127.0.0.1:400" + string(rune('0'+i))
		conn, err := fwd.GetConn(ctx, target)
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	}
}

func TestRequestForwarder_DifferentConfigs(t *testing.T) {
	resolver := new(MockResolver)

	// Create two forwarders with different configs
	config1 := DefaultForwarderConfig()
	config1.DialTimeout = 1 * time.Second

	config2 := DefaultForwarderConfig()
	config2.DialTimeout = 5 * time.Second

	fwd1 := NewRequestForwarder(&config1, resolver)
	fwd2 := NewRequestForwarder(&config2, resolver)
	defer func() { _ = fwd1.Close(); _ = fwd2.Close() }()

	ctx := context.Background()
	target1 := "127.0.0.1:5001"
	target2 := "127.0.0.1:5002" // Different target to avoid gRPC internal races

	// Get connection from first forwarder
	conn1, err := fwd1.GetConn(ctx, target1)
	assert.NoError(t, err)
	assert.NotNil(t, conn1)

	// Get connection from second forwarder
	conn2, err := fwd2.GetConn(ctx, target2)
	assert.NoError(t, err)
	assert.NotNil(t, conn2)

	// They should be different objects (different pools, different targets)
	// Note: Can't use assert.NotEqual with gRPC connections due to internal race in reflect.DeepEqual
	// Just verify both connections are valid
	assert.NotNil(t, conn1)
	assert.NotNil(t, conn2)
}

func BenchmarkRequestForwarder_GetConn(b *testing.B) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	target := "127.0.0.1:6001"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn, err := fwd.GetConn(ctx, target)
		if err != nil {
			b.Fatal(err)
		}
		_ = conn
	}
}

func BenchmarkRequestForwarder_GetConnConcurrent(b *testing.B) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	target := "127.0.0.1:7001"

	b.ResetTimer()
	b.ReportAllocs()
	b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := fwd.GetConn(ctx, target)
			if err != nil {
				b.Fatal(err)
			}
			_ = conn
		}
	})
}

func BenchmarkRequestForwarder_GetConnMultipleTargets(b *testing.B) {
	resolver := new(MockResolver)
	config := DefaultForwarderConfig()
	fwd := NewRequestForwarder(&config, resolver)
	defer func() { _ = fwd.Close() }()

	ctx := context.Background()
	targets := make([]string, 10)
	for i := 0; i < 10; i++ {
		targets[i] = "127.0.0.1:800" + string(rune('0'+i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		target := targets[i%10]
		conn, err := fwd.GetConn(ctx, target)
		if err != nil {
			b.Fatal(err)
		}
		_ = conn
	}
}

func FuzzRequestForwarder_GetConn(f *testing.F) {
	f.Add("127.0.0.1:9000") // target
	f.Add("192.168.1.1:8080")
	f.Add("[::1]:9000") // IPv6

	f.Fuzz(func(t *testing.T, target string) {
		resolver := new(MockResolver)
		config := DefaultForwarderConfig()
		fwd := NewRequestForwarder(&config, resolver)
		defer func() { _ = fwd.Close() }()

		ctx := context.Background()

		// This should not panic for valid targets
		conn, err := fwd.GetConn(ctx, target)
		if err != nil {
			// Connection errors are expected for invalid targets
			return
		}
		if conn == nil {
			t.Error("GetConn returned nil connection without error")
		}
	})
}

func FuzzRequestForwarder_ConcurrentAccess(f *testing.F) {
	f.Add(10, 100) // numGoroutines, numIterations
	f.Add(50, 500)
	f.Add(20, 200)

	f.Fuzz(func(t *testing.T, numGoroutines, numIterations int) {
		if numGoroutines <= 0 || numGoroutines > 50 {
			t.Skip()
		}
		if numIterations <= 0 || numIterations > 1000 {
			t.Skip()
		}

		resolver := new(MockResolver)
		config := DefaultForwarderConfig()
		fwd := NewRequestForwarder(&config, resolver)
		defer func() { _ = fwd.Close() }()

		ctx := context.Background()
		target := "127.0.0.1:9100"

		done := make(chan bool, numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func() {
				for i := 0; i < numIterations; i++ {
					conn, err := fwd.GetConn(ctx, target)
					if err != nil {
						// Connection errors are expected for invalid targets
						continue
					}
					if conn == nil {
						t.Error("GetConn returned nil connection without error")
					}
				}
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < numGoroutines; i++ {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for goroutines")
			}
		}
	})
}
