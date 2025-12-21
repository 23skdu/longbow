//go:build !linux

package store

import "unsafe"

func pinThreadToCoreLinux(core int) error {
	return nil
}

func getNumaNodeLinux(ptr unsafe.Pointer) (int, error) {
	return -1, nil
}

func pinThreadToNodeLinux(node int) error {
	return nil
}
