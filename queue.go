package queue

import (
	"errors"
	"sync"
)

var (
	ErrQueueIsEmpty = errors.New("queue: queue is empty")
)

type Queue[T any] interface {
	Push(item T)
	Pop() (T, error)
	Size() int
}

type ThreadSafeQueue[T any] struct {
	mutex *sync.RWMutex
	queue *UnsafeQueue[T]
}

func NewThreadSafeQueue[T any]() *ThreadSafeQueue[T] {
	return &ThreadSafeQueue[T]{mutex: &sync.RWMutex{}, queue: NewUnsafeQueue[T]()}
}

func (q *ThreadSafeQueue[T]) Push(item T) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.queue.Push(item)
}

func (q *ThreadSafeQueue[T]) Pop() (T, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.queue.Pop()
}

func (q *ThreadSafeQueue[T]) Size() int {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.queue.Size()
}

type UnsafeQueue[T any] struct {
	items []T
}

func NewUnsafeQueue[T any]() *UnsafeQueue[T] {
	return &UnsafeQueue[T]{items: make([]T, 0)}
}

func (q *UnsafeQueue[T]) Push(item T) {
	q.items = append(q.items, item)
}

func (q *UnsafeQueue[T]) Pop() (T, error) {
	if len(q.items) == 0 {
		var emptyEvent T
		return emptyEvent, ErrQueueIsEmpty
	}

	event := q.items[0]
	q.items = q.items[1:]
	return event, nil
}

func (q *UnsafeQueue[T]) Size() int {
	return len(q.items)
}
