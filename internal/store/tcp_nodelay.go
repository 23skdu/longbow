package store

import (
"net"
"sync/atomic"

"github.com/23skdu/longbow/internal/metrics"
)

// tcpNoDelayConnectionsTotal tracks connections with TCP_NODELAY set
var tcpNoDelayConnectionsTotal atomic.Int64

// TCPNoDelayListener wraps a TCPListener and sets TCP_NODELAY on all accepted connections.
// This disables Nagle's algorithm for lower latency at the cost of potential bandwidth efficiency.
type TCPNoDelayListener struct {
*net.TCPListener
}

// NewTCPNoDelayListener creates a new listener that sets TCP_NODELAY on accepted connections.
func NewTCPNoDelayListener(l *net.TCPListener) *TCPNoDelayListener {
return &TCPNoDelayListener{TCPListener: l}
}

// Accept waits for and returns the next connection, with TCP_NODELAY enabled.
func (l *TCPNoDelayListener) Accept() (net.Conn, error) {
conn, err := l.AcceptTCP()
if err != nil {
return nil, err
}

// Set TCP_NODELAY to disable Nagle's algorithm
if err := conn.SetNoDelay(true); err != nil {
// Log but don't fail - connection is still usable
_ = err
}

// Increment metrics
tcpNoDelayConnectionsTotal.Add(1)
metrics.TCPNoDelayConnectionsTotal.Inc()

return conn, nil
}

// GetTCPNoDelayConnectionsTotal returns the total number of connections with TCP_NODELAY set.
// This is useful for testing and monitoring.
func GetTCPNoDelayConnectionsTotal() int64 {
return tcpNoDelayConnectionsTotal.Load()
}
