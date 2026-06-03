//go:build linux

package gpu

import "golang.org/x/sys/unix"

const populatedMmapFlags = unix.MAP_ANONYMOUS | unix.MAP_POPULATE | unix.MAP_LOCKED
