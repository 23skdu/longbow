package store

// ChunkSize is provided by type_aliases.go

func chunkID(id uint32) int {
	return int(id / uint32(ChunkSize))
}

func chunkOffset(id uint32) int {
	return int(id % uint32(ChunkSize))
}
