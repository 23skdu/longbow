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

// UnregisterArena removes an arena from the global registry.
func UnregisterArena(a *SlabArena) {
	globalRegistryMu.Lock()
	defer globalRegistryMu.Unlock()
	for i, arena := range globalRegistry {
		if arena == a {
			globalRegistry[i] = globalRegistry[len(globalRegistry)-1]
			globalRegistry = globalRegistry[:len(globalRegistry)-1]
			return
		}
	}
}

// GetGlobalArenas returns a snapshot of all registered arenas.
func GetGlobalArenas() []*SlabArena {
	globalRegistryMu.RLock()
	defer globalRegistryMu.RUnlock()

	snapshot := make([]*SlabArena, len(globalRegistry))
	copy(snapshot, globalRegistry)
	return snapshot
}
