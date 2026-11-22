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

// NewStaticProvider constructs a new StaticProvider.
func NewStaticProvider(peers []string) *StaticProvider {
	return &StaticProvider{Peers: peers}
}

// FindPeers runs the find peers operation.
func (s *StaticProvider) FindPeers(ctx context.Context) ([]string, error) {
	return s.Peers, nil
}

// Register runs the register operation.
func (s *StaticProvider) Register(ctx context.Context, id string, port int) error {
	// No-op for static list
	return nil
}

// MultiProvider chains multiple providers (e.g., Static + MDNS).
type MultiProvider struct {
	Providers []DiscoveryProvider
}

// NewMultiProvider constructs a new MultiProvider.
func NewMultiProvider(providers ...DiscoveryProvider) *MultiProvider {
	return &MultiProvider{Providers: providers}
}

// FindPeers runs the find peers operation.
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

// Register runs the register operation.
func (m *MultiProvider) Register(ctx context.Context, id string, port int) error {
	for _, p := range m.Providers {
		_ = p.Register(ctx, id, port)
	}
	return nil
}
