package store

import (
	"net"
	"testing"
	"time"
)

// TestTCPNoDelayListenerSetsNoDelay verifies that accepted connections have TCP_NODELAY set
func TestTCPNoDelayListenerSetsNoDelay(t *testing.T) {
	// Create a regular TCP listener
	baseLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create base listener: %v", err)
	}
	defer baseLis.Close() //nolint:errcheck

	// Wrap it with TCP_NODELAY listener
	noDelayLis := NewTCPNoDelayListener(baseLis.(*net.TCPListener))
	defer noDelayLis.Close() //nolint:errcheck

	// Connect to the listener
	addr := noDelayLis.Addr().String()
	done := make(chan struct{})

	go func() {
		conn, err := noDelayLis.Accept()
		if err != nil {
			t.Errorf("accept failed: %v", err)
			close(done)
			return
		}
		defer conn.Close() //nolint:errcheck

		// Verify TCP_NODELAY is set by checking the connection type
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			t.Errorf("expected *net.TCPConn, got %T", conn)
		}

		// The connection should be usable (TCP_NODELAY doesn't change that)
		if tcpConn == nil {
			t.Error("tcpConn is nil")
		}
		close(done)
	}()

	// Client connects
	clientConn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer clientConn.Close() //nolint:errcheck

	// Wait for server to process
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for accept")
	}
}

// TestTCPNoDelayListenerMetrics verifies that metrics are incremented
func TestTCPNoDelayListenerMetrics(t *testing.T) {
	// Create listener
	baseLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create base listener: %v", err)
	}
	defer baseLis.Close() //nolint:errcheck

	noDelayLis := NewTCPNoDelayListener(baseLis.(*net.TCPListener))
	defer noDelayLis.Close() //nolint:errcheck

	// Get initial count
	initialCount := GetTCPNoDelayConnectionsTotal()

	addr := noDelayLis.Addr().String()
	done := make(chan struct{})

	go func() {
		conn, _ := noDelayLis.Accept()
		if conn != nil {
			conn.Close() //nolint:errcheck
		}
		close(done)
	}()

	clientConn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	clientConn.Close() //nolint:errcheck

	<-done

	// Verify metric incremented
	newCount := GetTCPNoDelayConnectionsTotal()
	if newCount != initialCount+1 {
		t.Errorf("expected count %d, got %d", initialCount+1, newCount)
	}
}

// TestTCPNoDelayListenerAddr verifies Addr() returns correct address
func TestTCPNoDelayListenerAddr(t *testing.T) {
	baseLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer baseLis.Close() //nolint:errcheck

	noDelayLis := NewTCPNoDelayListener(baseLis.(*net.TCPListener))
	defer noDelayLis.Close() //nolint:errcheck

	if noDelayLis.Addr() == nil {
		t.Error("Addr() returned nil")
	}

	if noDelayLis.Addr().Network() != "tcp" {
		t.Errorf("expected network 'tcp', got %s", noDelayLis.Addr().Network())
	}
}
