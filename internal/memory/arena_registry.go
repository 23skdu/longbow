package memory

import (
	"sync"
)

var (
	globalRegistry   []*SlabArena
	globalRegistryMu sync.RWMutex
)

// RegisterArena adds an arena to the global registry.
func RegisterArena(a *SlabArena) {
	globalRegistryMu.Lock()
	defer globalRegistryMu.Unlock()
	globalRegistry = append(globalRegistry, a)
}

// GetGlobalArenas returns a snapshot of all registered arenas.
func GetGlobalArenas() []*SlabArena {
	globalRegistryMu.RLock()
	defer globalRegistryMu.RUnlock()

	snapshot := make([]*SlabArena, len(globalRegistry))
	copy(snapshot, globalRegistry)
	return snapshot
}
