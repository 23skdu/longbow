package store

import (
	engine "github.com/23skdu/longbow/internal/store/engine"
	"github.com/apache/arrow-go/v18/arrow"
)

// LockFreeSlice is a lock-free slice re-exported from the store engine.
type LockFreeSlice[T any] = engine.LockFreeSlice[T]

// LockFreeMap is a lock-free map re-exported from the store engine.
type LockFreeMap[K comparable, V any] = engine.LockFreeMap[K, V]

// LockFreeRingBuffer is a lock-free ring buffer re-exported from the store engine.
type LockFreeRingBuffer[T any] = engine.LockFreeRingBuffer[T]

// Quantizer is the generic quantizer interface re-exported from the store engine.
type Quantizer[T engine.VectorType, U any] = engine.Quantizer[T, U]

// NewLockFreeSlice constructs a lock-free slice.
func NewLockFreeSlice[T any]() *LockFreeSlice[T] { return engine.NewLockFreeSlice[T]() }

// NewLockFreeSliceFrom constructs a lock-free slice from items.
func NewLockFreeSliceFrom[T any](items []T) *LockFreeSlice[T] {
	return engine.NewLockFreeSliceFrom[T](items)
}

// NewLockFreeMap constructs a lock-free map.
func NewLockFreeMap[K comparable, V any]() *LockFreeMap[K, V] { return engine.NewLockFreeMap[K, V]() }

// ExtractVectorGeneric extracts a typed vector from an Arrow record batch.
func ExtractVectorGeneric[T any](rec arrow.RecordBatch, rowIdx, colIdx int) ([]T, error) {
	return engine.ExtractVectorGeneric[T](rec, rowIdx, colIdx)
}
