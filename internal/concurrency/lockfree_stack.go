package concurrency

import (
	"sync/atomic"
)

type LockFreeStack[T any] struct {
	head atomic.Value
}

type node[T any] struct {
	value T
	next  *node[T]
}

func NewLockFreeStack[T any]() *LockFreeStack[T] {
	return &LockFreeStack[T]{}
}

func (s *LockFreeStack[T]) Push(value T) {
	newNode := &node[T]{value: value}

	for {
		head := s.head.Load()
		if head == nil {
			if s.head.CompareAndSwap(nil, newNode) {
				return
			}
			continue
		}

		headVal := head.(*node[T])
		newNode.next = headVal

		if s.head.CompareAndSwap(head, newNode) {
			return
		}
	}
}

func (s *LockFreeStack[T]) Pop() (T, bool) {
	var zero T

	for {
		head := s.head.Load()
		if head == nil {
			return zero, false
		}

		headVal := head.(*node[T])
		next := headVal.next

		if s.head.CompareAndSwap(head, next) {
			return headVal.value, true
		}
	}
}
