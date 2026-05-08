package memory

import (
	"sync"
)

var (
	globalRegistry   []*ArenaStatsRecord
	globalRegistryMu sync.RWMutex
)

// RegisterArena adds an arena's stats record to the global registry.
func RegisterArena(s *ArenaStatsRecord) {
	globalRegistryMu.Lock()
	defer globalRegistryMu.Unlock()
	globalRegistry = append(globalRegistry, s)
}

// UnregisterArena removes an arena's stats record from the global registry.
func UnregisterArena(s *ArenaStatsRecord) {
	globalRegistryMu.Lock()
	defer globalRegistryMu.Unlock()
	for i, record := range globalRegistry {
		if record == s {
			globalRegistry[i] = globalRegistry[len(globalRegistry)-1]
			globalRegistry = globalRegistry[:len(globalRegistry)-1]
			return
		}
	}
}

// GetGlobalArenas returns a snapshot of all registered arena stats records.
func GetGlobalArenas() []*ArenaStatsRecord {
	globalRegistryMu.RLock()
	defer globalRegistryMu.RUnlock()

	snapshot := make([]*ArenaStatsRecord, len(globalRegistry))
	copy(snapshot, globalRegistry)
	return snapshot
}
