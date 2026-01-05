package mesh

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper to find free port
func getFreePort() int {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	l, err := net.ListenUDP("udp", addr)
	if err != nil {
		return 0
	}
	defer func() { _ = l.Close() }()
	return l.LocalAddr().(*net.UDPAddr).Port
}

func TestSuspicionTimeout(t *testing.T) {
	// 1. Setup Node A and Node B
	portA := getFreePort()
	portB := getFreePort()

	cfgA := GossipConfig{
		ID:               "nodeA",
		Port:             portA,
		ProtocolPeriod:   100 * time.Millisecond,
		SuspicionTimeout: 500 * time.Millisecond, // Fast timeout
		AckTimeout:       50 * time.Millisecond,
	}
	nodeA := NewGossip(cfgA)
	require.NoError(t, nodeA.Start())
	defer nodeA.Stop()

	cfgB := GossipConfig{
		ID:               "nodeB",
		Port:             portB,
		ProtocolPeriod:   100 * time.Millisecond,
		SuspicionTimeout: 500 * time.Millisecond,
		AckTimeout:       50 * time.Millisecond,
	}
	nodeB := NewGossip(cfgB)
	require.NoError(t, nodeB.Start())
	defer nodeB.Stop()

	// 2. A Joins B
	err := nodeA.Join(fmt.Sprintf("127.0.0.1:%d", portB))
	require.NoError(t, err)

	// Wait for convergence
	require.Eventually(t, func() bool {
		m := nodeA.GetMembers()
		return len(m) == 2
	}, 2*time.Second, 100*time.Millisecond, "Node A should see Node B")

	// 3. Stop Node B (Simulate Failure)
	nodeB.Stop()

	// 4. Verify A marks B as Suspect
	require.Eventually(t, func() bool {
		members := nodeA.GetMembers()
		for _, m := range members {
			if m.ID == "nodeB" && m.Status == StatusSuspect {
				return true
			}
		}
		return false
	}, 2*time.Second, 100*time.Millisecond, "Node A should mark Node B as Suspect")

	// 5. Verify A marks B as Dead after timeout
	// Suspicion timeout is 500ms. Wait slightly longer.
	require.Eventually(t, func() bool {
		members := nodeA.GetMembers()
		for _, m := range members {
			if m.ID == "nodeB" && m.Status == StatusDead {
				// Ensure suspect timer worked
				// Check SuspectAt? Not visible in GetMembers result easily unless checking internal state or exported struct
				return true
			}
		}
		return false
	}, 2*time.Second, 100*time.Millisecond, "Node A should mark Node B as Dead")
}

func TestRefutation(t *testing.T) {
	// 1. Setup Node A and Node B
	portA := getFreePort()
	portB := getFreePort()

	cfgA := GossipConfig{ID: "nodeA", Port: portA, ProtocolPeriod: 100 * time.Millisecond, SuspicionTimeout: 2 * time.Second}
	nodeA := NewGossip(cfgA)
	require.NoError(t, nodeA.Start())
	defer nodeA.Stop()

	cfgB := GossipConfig{ID: "nodeB", Port: portB, ProtocolPeriod: 100 * time.Millisecond, SuspicionTimeout: 2 * time.Second}
	nodeB := NewGossip(cfgB)
	require.NoError(t, nodeB.Start())
	defer nodeB.Stop()

	// Join bidirectional to ensure both know each other
	require.NoError(t, nodeA.Join(fmt.Sprintf("127.0.0.1:%d", portB)))
	require.NoError(t, nodeB.Join(fmt.Sprintf("127.0.0.1:%d", portA)))

	// Wait for convergence ensuring nodeA knows nodeB
	require.Eventually(t, func() bool {
		m := nodeA.GetMembers()
		for _, mem := range m {
			if mem.ID == "nodeB" && mem.Status == StatusAlive {
				return true
			}
		}
		return false
	}, 2*time.Second, 100*time.Millisecond, "Node A should know Node B")

	// 2. Artificially mark B as Suspect on Node A
	nodeA.markSuspect("nodeB")

	// 3. Node B should receive this (via gossip/ping) and refute it by incrementing incarnation
	// We check Node A's view of B eventually becoming Alive again with higher incarnation

	require.Eventually(t, func() bool {
		members := nodeA.GetMembers()
		for _, m := range members {
			if m.ID == "nodeB" {
				if m.Status == StatusAlive && m.Incarnation > 1 {
					return true
				}
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "Node B should refute suspicion logic and become Alive with Incarnation > 1")
}
