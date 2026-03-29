//go:build !linux

package gpu

import "golang.org/x/sys/unix"

// populatedMmapFlags on non-Linux platforms (no MAP_POPULATE support in unix package usually)
const populatedMmapFlags = unix.MAP_ANONYMOUS
