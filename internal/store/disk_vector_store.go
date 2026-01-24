package store

import (
	"sync"
)

type DiskVectorStore struct {
	mu   sync.RWMutex
	path string
	dim  int
}

func NewDiskVectorStore(path string, dim int) (*DiskVectorStore, error) {
	return &DiskVectorStore{
		path: path,
		dim:  dim,
	}, nil
}

func (dvs *DiskVectorStore) Close() error {
	return nil
}

func (dvs *DiskVectorStore) BatchAppend(vectors [][]float32) (int, error) {
	return len(vectors), nil
}
