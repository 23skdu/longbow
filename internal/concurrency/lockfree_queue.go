package concurrency

import (
	"sync/atomic"
	"unsafe"
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
	dummy := node[T]{}
	q := &LockFreeQueue[T]{
		dummy: dummy,
	}
	q.head.Store(unsafe.Pointer(&dummy))
	q.tail.Store(unsafe.Pointer(&dummy))
	return q
}

func (q *LockFreeQueue[T]) Enqueue(value T) {
	newNode := &node[T]{value: value}

	for {
		tail := q.tail.Load()
		next := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(&tail.next)))

		if tail != q.tail.Load() {
			continue
		}

		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(&tail.next), unsafe.Pointer(next), unsafe.Pointer(newNode)) {
			q.tail.Store(unsafe.Pointer(newNode))
			return
		}
	}
}

func (q *LockFreeQueue[T]) Dequeue() (T, bool) {
	var zero T

	for {
		head := q.head.Load()
		tail := q.tail.Load()
		next := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(&head.next)))

		if head == q.head.Load() {
			if head == tail {
				return zero, false
			}
			if next == nil {
				return zero, false
			}

			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(&q.head), unsafe.Pointer(head), unsafe.Pointer(next)) {
				return next.value, true
			}
		}
	}
}

func (q *LockFreeQueue[T]) IsEmpty() bool {
	head := q.head.Load()
	tail := q.tail.Load()
	next := (*node[T])(atomic.LoadPointer((*unsafe.Pointer)(&head.next)))
	return head == tail && next == nil
}
