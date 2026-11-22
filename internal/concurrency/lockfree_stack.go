package concurrency

import (
	"sync/atomic"
)

// LockFreeStack represents a lock free stack value.
type LockFreeStack[T any] struct {
	head atomic.Value
}

type stackNode[T any] struct {
	value T
	next  *stackNode[T]
}

// NewLockFreeStack constructs a new LockFreeStack.
func NewLockFreeStack[T any]() *LockFreeStack[T] {
	return &LockFreeStack[T]{}
}

// Push runs the push operation.
func (s *LockFreeStack[T]) Push(value T) {
	newNode := &stackNode[T]{value: value}

	for {
		head := s.head.Load()
		if head == nil {
			newNode.next = nil
		} else {
			headNode := head.(*stackNode[T])
			newNode.next = headNode
		}

		if s.head.CompareAndSwap(head, newNode) {
			return
		}
	}
}

// Pop runs the pop operation.
func (s *LockFreeStack[T]) Pop() (T, bool) {
	var zero T

	for {
		head := s.head.Load()
		if head == nil {
			return zero, false
		}

		headNode := head.(*stackNode[T])
		if headNode == nil {
			return zero, false
		}

		next := headNode.next

		if s.head.CompareAndSwap(head, next) {
			return headNode.value, true
		}
	}
}
