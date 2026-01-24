package concurrency

import (
	"sync/atomic"
)

type LockFreeQueue[T any] struct {
	head  atomic.Pointer[node[T]]
	tail  atomic.Pointer[node[T]]
	dummy node[T]
}

type node[T any] struct {
	value T
	next  atomic.Pointer[node[T]]
}

func NewLockFreeQueue[T any]() *LockFreeQueue[T] {
	q := &LockFreeQueue[T]{}
	q.head.Store(&q.dummy)
	q.tail.Store(&q.dummy)
	return q
}

func (q *LockFreeQueue[T]) Enqueue(value T) {
	newNode := &node[T]{value: value}

	for {
		tail := q.tail.Load()

		if tail != q.tail.Load() {
			continue
		}

		if tail.next.CompareAndSwap(nil, newNode) {
			q.tail.Store(newNode)
			return
		}
	}
}

func (q *LockFreeQueue[T]) Dequeue() (T, bool) {
	var zero T

	for {
		head := q.head.Load()
		tail := q.tail.Load()
		next := head.next.Load()

		if head == q.head.Load() {
			if head == tail {
				return zero, false
			}
			if next == nil {
				return zero, false
			}

			if q.head.CompareAndSwap(head, next) {
				return next.value, true
			}
		}
	}
}

func (q *LockFreeQueue[T]) IsEmpty() bool {
	head := q.head.Load()
	tail := q.tail.Load()
	next := head.next.Load()
	return head == tail && next == nil
}
