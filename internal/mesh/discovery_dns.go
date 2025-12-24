package mesh

import (
	"context"
	"fmt"
	"net"
)

// DNSProvider performs peer discovery using DNS A/AAAA records.
// It is useful for headless services in Kubernetes.
type DNSProvider struct {
	Record string
	Port   int
}

// NewDNSProvider creates a new DNS discovery provider.
func NewDNSProvider(record string, port int) *DNSProvider {
	return &DNSProvider{
		Record: record,
		Port:   port,
	}
}

// FindPeers resolves the configured DNS record and returns a list of peer addresses.
func (d *DNSProvider) FindPeers(ctx context.Context) ([]string, error) {
	// Use default resolver
	// Note: net.LookupIP allocates a slice of IPs.
	// We cannot easily do zero-alloc DNS without a custom raw DNS client,
	// which is overkill and requires raw socket permissions.
	// We minimize allocations by pre-allocating the result slice if possible (not possible here).
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", d.Record)
	if err != nil {
		return nil, err
	}

	peers := make([]string, 0, len(ips))
	for _, ip := range ips {
		// Detect if IPv4 or IPv6 and format accordingly
		// This allocation (fmt.Sprintf) is unavoidable for returning strings,
		// but necessary for the current interface.
		if ip4 := ip.To4(); ip4 != nil {
			peers = append(peers, fmt.Sprintf("%s:%d", ip4.String(), d.Port))
		} else {
			// IPv6
			peers = append(peers, fmt.Sprintf("[%s]:%d", ip.String(), d.Port))
		}
	}

	return peers, nil
}

// Register is a no-op for DNS provider as records are typically managed externally.
func (d *DNSProvider) Register(ctx context.Context, id string, port int) error {
	return nil
}
