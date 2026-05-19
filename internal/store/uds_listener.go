package store

import (
	"net"

	"github.com/23skdu/longbow/internal/metrics"
)

// UDSListener wraps a Unix Domain Socket listener and increments a counter for accepted connections.
type UDSListener struct {
	net.Listener
}

// NewUDSListener creates a new listener that counts UDS connections.
func NewUDSListener(l net.Listener) *UDSListener {
	return &UDSListener{Listener: l}
}

// Accept waits for and returns the next connection, incrementing the UDS connection counter.
func (l *UDSListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	// Increment metrics
	metrics.UDSConnectionsTotal.Inc()

	return conn, nil
}
