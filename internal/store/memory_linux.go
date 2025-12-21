//go:build linux

package store

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

func pinThreadToCoreLinux(core int) error {
	var cpuSet unix.CPUSet
	cpuSet.Set(core)
	return unix.SchedSetaffinity(0, &cpuSet)
}

func getNumaNodeLinux(ptr unsafe.Pointer) (int, error) {
	// Use MovePages with nil nodes to query status
	pages := []unsafe.Pointer{ptr}
	status := make([]int, 1)

	// Direct syscall implementation since x/sys/unix missing MovePages on some versions
	if err := movePages(0, pages, nil, status, 2); err != nil { // 2 = MPOL_MF_MOVE
		return -1, err
	}

	// status[0] contains the node ID
	return status[0], nil
}

func movePages(pid int, pages []unsafe.Pointer, nodes []int, status []int, flags int) error {
	var pPages, pNodes, pStatus uintptr

	if len(pages) > 0 {
		pPages = uintptr(unsafe.Pointer(&pages[0]))
	}
	if len(nodes) > 0 {
		pNodes = uintptr(unsafe.Pointer(&nodes[0]))
	}
	if len(status) > 0 {
		pStatus = uintptr(unsafe.Pointer(&status[0]))
	}

	_, _, e1 := unix.Syscall6(
		unix.SYS_MOVE_PAGES,
		uintptr(pid),
		uintptr(len(pages)),
		pPages,
		pNodes,
		pStatus,
		uintptr(flags),
	)
	if e1 != 0 {
		return e1
	}
	return nil
}

func pinThreadToNodeLinux(node int) error {
	// Parse /sys/devices/system/node/nodeX/cpulist to get CPU list
	cpulistPath := fmt.Sprintf("/sys/devices/system/node/node%d/cpulist", node)

	// Ensure we lock the OS thread so this pinning sticks to the goroutine's thread
	runtime.LockOSThread()

	f, err := os.Open(cpulistPath)
	if err != nil {
		return fmt.Errorf("failed to open cpulist: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return fmt.Errorf("empty cpulist")
	}
	line := scanner.Text()

	// Parse generic CPU list format (e.g., "0-3,8-11")
	cpus, err := parseCPUList(line)
	if err != nil {
		return err
	}

	var cpuSet unix.CPUSet
	for _, cpu := range cpus {
		cpuSet.Set(cpu)
	}

	return unix.SchedSetaffinity(0, &cpuSet)
}

// parseCPUList parses linux cpulist format "0-3,8,10-12"
func parseCPUList(list string) ([]int, error) {
	var cpus []int
	parts := strings.Split(list, ",")
	for _, part := range parts {
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range: %s", part)
			}
			start, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			if err != nil {
				return nil, err
			}
			end, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err != nil {
				return nil, err
			}
			for i := start; i <= end; i++ {
				cpus = append(cpus, i)
			}
		} else {
			cpu, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				return nil, err
			}
			cpus = append(cpus, cpu)
		}
	}
	return cpus, nil
}
