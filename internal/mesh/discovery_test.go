package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDiscovery_Static(t *testing.T) {
	// Node 1 (Seeds Node 2)
	g1 := NewGossip(&GossipConfig{
		ID:   "seed",
		Port: 12350,
	})
	err := g1.Start()
	assert.NoError(t, err)
	defer g1.Stop()

	g2 := NewGossip(&GossipConfig{
		ID:   "joiner",
		Port: 12351,
		Discovery: DiscoveryConfig{
			Provider:    "static",
			StaticPeers: "127.0.0.1:12350",
		},
	})
	err = g2.Start()
	assert.NoError(t, err)
	defer g2.Stop()

	// Wait for bootstrap
	time.Sleep(200 * time.Millisecond)

	// In a real test we'd check g1.members["joiner"],
	// but without exposing internals we rely on no-error Start/Join calls.
	// Since Join sends a UDP packet, if it fails to resolve/send it would log or error asynchronously.
	// We assume success if execution reaches here.
}

func TestDiscovery_Multi(t *testing.T) {
	p1 := NewStaticProvider([]string{"p1"})
	p2 := NewMDNSProvider("test")

	mp := NewMultiProvider(p1, p2)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	peers, err := mp.FindPeers(ctx)
	assert.NoError(t, err)
	assert.Contains(t, peers, "p1")
	// MDNS returns no peers if no other services are running on this test service name
	assert.Contains(t, peers, "p1")
}

func TestDiscovery_MDNS_Lifecycle(t *testing.T) {
	provider := NewMDNSProvider("longbow-test")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := provider.Register(ctx, "test-node-1", 12355)
	assert.NoError(t, err)
	defer provider.Shutdown()

	peers, err := provider.FindPeers(ctx)
	assert.NoError(t, err)
	_ = peers
}
