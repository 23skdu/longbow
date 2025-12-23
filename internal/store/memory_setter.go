package store

// SetMemoryConfig updates the memory configuration
func (s *VectorStore) SetMemoryConfig(cfg MemoryConfig) {
	s.memoryConfig = cfg
	s.maxMemory.Store(cfg.MaxMemory)
}
