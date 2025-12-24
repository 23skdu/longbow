package mesh

import (
	"context"
	"fmt"
	"time"
)

func (g *Gossip) discoveryLoop() {
	// Initial delay to let things settle
	time.Sleep(500 * time.Millisecond)

	// Register once (retrying if needed could be added, but usually one-off)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = g.discoveryProvider.Register(ctx, g.Config.ID, g.Config.Port)
	cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Run immediately first
	g.runDiscovery()

	for {
		select {
		case <-g.closeCh:
			return
		case <-ticker.C:
			g.runDiscovery()
		}
	}
}

func (g *Gossip) runDiscovery() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peers, err := g.discoveryProvider.FindPeers(ctx)
	if err != nil {
		return
	}

	selfAddr := fmt.Sprintf("127.0.0.1:%d", g.Config.Port)

	for _, peer := range peers {
		// Don't join self
		if peer == selfAddr {
			continue
		}
		
		// Optimization: Check if already known and alive to avoid spamming Join
		g.mu.RLock()
		existing := false
		for _, m := range g.members {
			if m.Addr == peer && m.Status == StatusAlive {
				existing = true
				break
			}
		}
		g.mu.RUnlock()

		if !existing {
			go g.Join(peer)
		}
	}
}
