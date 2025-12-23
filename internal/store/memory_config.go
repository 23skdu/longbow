package store

// MemoryConfig defines memory management behavior
type MemoryConfig struct {
	MaxMemory        int64   // Maximum memory in bytes
	EvictionHeadroom float64 // Percentage headroom before eviction (0.10 = 10%)
	EvictionPolicy   string  // "lru", "oldest", "largest"
	RejectWrites     bool    // Reject writes when limit hit (vs attempt eviction)
}

// DefaultMemoryConfig returns sensible defaults
func DefaultMemoryConfig() MemoryConfig {
	return MemoryConfig{
		MaxMemory:        1073741824, // 1GB
		EvictionHeadroom: 0.10,       // 10% headroom
		EvictionPolicy:   "lru",
		RejectWrites:     true,
	}
}

// EvictionThreshold returns the memory threshold at which eviction should trigger
func (c MemoryConfig) EvictionThreshold() int64 {
	return int64(float64(c.MaxMemory) * (1.0 - c.EvictionHeadroom))
}
