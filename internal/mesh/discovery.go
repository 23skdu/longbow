package mesh

import (
	"context"
)

// DiscoveryProvider finds peer addresses to bootstrap the mesh.
type DiscoveryProvider interface {
	// FindPeers returns a list of peer addresses (host:port).
	FindPeers(ctx context.Context) ([]string, error)
	// Register announces this node to the discovery backend (if applicable).
	Register(ctx context.Context, id string, port int) error
}

// StaticProvider returns a fixed list of peers.
type StaticProvider struct {
	Peers []string
}

func NewStaticProvider(peers []string) *StaticProvider {
	return &StaticProvider{Peers: peers}
}

func (s *StaticProvider) FindPeers(ctx context.Context) ([]string, error) {
	return s.Peers, nil
}

func (s *StaticProvider) Register(ctx context.Context, id string, port int) error {
	// No-op for static list
	return nil
}

// MultiProvider chains multiple providers (e.g., Static + MDNS).
type MultiProvider struct {
	Providers []DiscoveryProvider
}

func NewMultiProvider(providers ...DiscoveryProvider) *MultiProvider {
	return &MultiProvider{Providers: providers}
}

func (m *MultiProvider) FindPeers(ctx context.Context) ([]string, error) {
	var allPeers []string
	seen := make(map[string]bool)

	for _, p := range m.Providers {
		peers, err := p.FindPeers(ctx)
		if err != nil {
			continue // Log error?
		}
		for _, peer := range peers {
			if !seen[peer] {
				allPeers = append(allPeers, peer)
				seen[peer] = true
			}
		}
	}
	return allPeers, nil
}

func (m *MultiProvider) Register(ctx context.Context, id string, port int) error {
	for _, p := range m.Providers {
		_ = p.Register(ctx, id, port)
	}
	return nil
}
