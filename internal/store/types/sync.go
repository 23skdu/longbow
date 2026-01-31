package types

import "github.com/23skdu/longbow/internal/core"

// SyncState represents the serializable state of a vector index.
type SyncState struct {
	Version   uint64
	Dims      int
	Locations []core.Location
	GraphData []byte
}

// DeltaSync represents incremental changes between versions.
type DeltaSync struct {
	FromVersion  uint64
	ToVersion    uint64
	NewLocations []core.Location
	StartIndex   int
}
