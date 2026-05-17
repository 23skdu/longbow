//go:build linux
// +build linux

package store

import (
	"net"
	"syscall"
)

// setQuickAck enables TCP_QUICKACK on Linux to reduce acknowledgment latency.
func setQuickAck(conn *net.TCPConn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	err = rawConn.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_QUICKACK, 1)
	})
	if err != nil {
		return err
	}
	return setErr
}
