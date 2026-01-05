package mesh

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
)

// MDNSProvider uses Multicast DNS to find local peers.
type MDNSProvider struct {
	Service string
	Domain  string

	server *zeroconf.Server
	mu     sync.Mutex
}

func NewMDNSProvider(service string) *MDNSProvider {
	// Ensure service format is correct (e.g., _service._tcp)
	if !strings.HasPrefix(service, "_") {
		service = "_" + service
	}
	if !strings.Contains(service, "._tcp") && !strings.Contains(service, "._udp") {
		service = service + "._tcp"
	}

	return &MDNSProvider{
		Service: service,
		Domain:  "local.",
	}
}

func (m *MDNSProvider) FindPeers(ctx context.Context) ([]string, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create resolver: %w", err)
	}

	entries := make(chan *zeroconf.ServiceEntry)
	var peers []string
	var errBrowse error

	// Create a child context to ensure browsing stops if ctx doesn't expire quickly?
	// The interface implies we should return eventually. If ctx is Background, we might hang.
	// But usually FindPeers is called with a timeout.
	// We'll trust ctx.

	go func() {
		// Browse blocks until context is cancelled
		if err := resolver.Browse(ctx, m.Service, m.Domain, entries); err != nil {
			errBrowse = err
		}
		// zeroconf closes the channel when finished/cancelled
	}()

	for entry := range entries {
		// Prefer IPv4
		port := entry.Port
		for _, ip := range entry.AddrIPv4 {
			if ip.IsLoopback() {
				// Optional: filter loopback if mesh should be external?
				// But for local dev loopback is useful.
				// Keep it.
			}
			peers = append(peers, fmt.Sprintf("%s:%d", ip.String(), port))
		}
	}

	if errBrowse != nil {
		return nil, errBrowse
	}

	return peers, nil
}

func (m *MDNSProvider) Register(ctx context.Context, id string, port int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server != nil {
		m.server.Shutdown()
		m.server = nil
	}

	// Instance name (id) must be provided
	if id == "" {
		id = "longbow-" + fmt.Sprintf("%d", time.Now().UnixNano())
	}

	// Register the service
	// We capture the server instance to stop it later (though interface doesn't expose Stop explicitely)
	server, err := zeroconf.Register(id, m.Service, m.Domain, port, []string{"version=1"}, nil)
	if err != nil {
		return fmt.Errorf("failed to register mdns service: %w", err)
	}
	m.server = server

	return nil
}

// Shutdown stops the mDNS server.
// Note: This method is not part of DiscoveryProvider interface but useful for cleanup.
func (m *MDNSProvider) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server != nil {
		m.server.Shutdown()
		m.server = nil
	}
}
