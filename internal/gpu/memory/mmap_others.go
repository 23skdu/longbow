//go:build !linux

package memory

import "golang.org/x/sys/unix"

const populatedMmapFlags = unix.MAP_ANONYMOUS
