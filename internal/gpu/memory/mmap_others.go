//go:build !linux && !windows

package memory

import "golang.org/x/sys/unix"

const populatedMmapFlags = unix.MAP_ANONYMOUS

var _ = populatedMmapFlags
