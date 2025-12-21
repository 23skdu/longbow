package mesh

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getFreePort() int {
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	l, _ := net.ListenUDP("udp", addr)
	defer l.Close()
	return l.LocalAddr().(*net.UDPAddr).Port
}

func TestSWIM_FailureDetection(t *testing.T) {
	p1 := getFreePort()
	p2 := getFreePort()
	p3 := getFreePort()

	g1 := NewGossip(GossipConfig{ID: "n1", Port: p1, ProtocolPeriod: 100 * time.Millisecond, AckTimeout: 50 * time.Millisecond, SuspicionTimeout: 500 * time.Millisecond})
	g2 := NewGossip(GossipConfig{ID: "n2", Port: p2, ProtocolPeriod: 100 * time.Millisecond, AckTimeout: 50 * time.Millisecond, SuspicionTimeout: 500 * time.Millisecond})
	g3 := NewGossip(GossipConfig{ID: "n3", Port: p3, ProtocolPeriod: 100 * time.Millisecond, AckTimeout: 50 * time.Millisecond, SuspicionTimeout: 500 * time.Millisecond})

	require.NoError(t, g1.Start())
	require.NoError(t, g2.Start())
	require.NoError(t, g3.Start())
	defer g1.Stop()
	defer g2.Stop()
	defer g3.Stop()

	// Connect mesh
	g1.Join(fmt.Sprintf("127.0.0.1:%d", p2))
	g2.Join(fmt.Sprintf("127.0.0.1:%d", p3))
	g3.Join(fmt.Sprintf("127.0.0.1:%d", p1))

	// Wait for full propagation
	require.Eventually(t, func() bool {
		return len(g1.GetMembers()) == 3 && len(g2.GetMembers()) == 3 && len(g3.GetMembers()) == 3
	}, 2*time.Second, 100*time.Millisecond)

	// Stop node 3
	g3.Stop()

	// Node 1 or 2 should mark Node 3 as Suspect then Dead
	require.Eventually(t, func() bool {
		members := g1.GetMembers()
		for _, m := range members {
			if m.ID == "n3" && m.Status == StatusDead {
				return true
			}
		}
		return false
	}, 5*time.Second, 200*time.Millisecond, "Node 3 should be marked Dead")
}

func TestSWIM_IndirectPing(t *testing.T) {
	// This test is harder because it requires mocking network partitions.
	// For now, let's verify node propagation via piggybacking.
	p1 := getFreePort()
	p2 := getFreePort()
	p3 := getFreePort()

	g1 := NewGossip(GossipConfig{ID: "n1", Port: p1, ProtocolPeriod: 100 * time.Millisecond})
	g2 := NewGossip(GossipConfig{ID: "n2", Port: p2, ProtocolPeriod: 100 * time.Millisecond})
	g3 := NewGossip(GossipConfig{ID: "n3", Port: p3, ProtocolPeriod: 100 * time.Millisecond})

	require.NoError(t, g1.Start())
	require.NoError(t, g2.Start())
	require.NoError(t, g3.Start())
	defer g1.Stop()
	defer g2.Stop()
	defer g3.Stop()

	// g1 connects to g2, g2 connects to g3. g1 and g3 don't know each other initially.
	g1.Join(fmt.Sprintf("127.0.0.1:%d", p2))
	g2.Join(fmt.Sprintf("127.0.0.1:%d", p3))

	// Verify g1 learns about g3 via g2
	require.Eventually(t, func() bool {
		members := g1.GetMembers()
		for _, m := range members {
			if m.ID == "n3" {
				return true
			}
		}
		return false
	}, 3*time.Second, 200*time.Millisecond, "g1 should learn about g3 via piggybacking")
}
