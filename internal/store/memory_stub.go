//go:build !linux

package store

import "unsafe"

func pinThreadToCoreLinux(_ int) error {
	return nil
}

func getNumaNodeLinux(_ unsafe.Pointer) (int, error) {
	return -1, nil
}

func pinThreadToNodeLinux(_ int) error {
	return nil
}
