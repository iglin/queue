package queue

import (
	"sync"
	"time"
)

type EventQueue[T any] interface {
	Scheduler[T]
	Subscribe(eventHandlerFunc EventHandler[T])
}

type EventHandler[T any] func(event T)

type EventQueueOpts struct {
	MaxParallelProcessors int
	BufferSize            int
	Delay                 time.Duration
}

type eventQueue[T any] struct {
	Scheduler[T]
	opts EventQueueOpts

	subMutex *sync.Mutex
	subs     []EventHandler[T]

	workerPool *WorkerPool
}

func NewEventQueue[T any](opts EventQueueOpts) EventQueue[T] {
	if opts.MaxParallelProcessors <= 0 {
		opts.MaxParallelProcessors = 1
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = DefaultBufferSize
	}
	queue := &eventQueue[T]{
		opts:       opts,
		subMutex:   &sync.Mutex{},
		subs:       make([]EventHandler[T], 0),
		workerPool: NewWorkerPool(opts.MaxParallelProcessors, opts.BufferSize),
	}
	queue.Scheduler = NewScheduler[T](queue.processEvent, queue.finalizeEventQueue, opts.BufferSize)
	return queue
}

func (q *eventQueue[T]) Subscribe(eventHandlerFunc EventHandler[T]) {
	q.subMutex.Lock()
	defer q.subMutex.Unlock()

	q.subs = append(q.subs, eventHandlerFunc)
}

func (q *eventQueue[T]) processEvent(getAndRemoveItem func() T) {
	time.Sleep(q.opts.Delay)

	event := getAndRemoveItem()

	q.subMutex.Lock()
	defer q.subMutex.Unlock()

	for _, sub := range q.subs {
		if err := q.workerPool.Schedule(func() {
			sub(event)
		}); err != nil {
			panic(err) // TODO: log here
		}
	}
}

func (q *eventQueue[T]) finalizeEventQueue() {
	if err := q.workerPool.Shutdown(0); err != nil {
		panic(err) // TODO: logging here
	}
}
