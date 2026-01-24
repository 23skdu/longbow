package store

import "github.com/23skdu/longbow/internal/store/types"

const ChunkSize = types.ChunkSize

func chunkID(id uint32) int {
	return int(id / uint32(ChunkSize))
}

func chunkOffset(id uint32) int {
	return int(id % uint32(ChunkSize))
}
