package store

import (
	"net"
)

// setQuickAck is a no-op on non-linux platforms.
func setQuickAck(_ *net.TCPConn) error {
	return nil
}
