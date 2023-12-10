package queue

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrDoesntAcceptTasks = errors.New("queue: scheduler shutdown function was called so it does not accept new items")
	ErrGracefulShutdown  = errors.New("queue: scheduler was not shut down during the grace period - there are still items in the queue that will be dropped")
)

type Scheduler[T any] interface {
	Schedule(item T) error
	Shutdown(gracefulPeriod time.Duration, pollingDelay ...time.Duration) error
}

type lifecycleEvent int

const (
	DefaultBufferSize = 100

	ItemAdded lifecycleEvent = iota
	Shutdown
)

type scheduler[T any] struct {
	acceptNewItems *atomic.Bool

	queue *ThreadSafeQueue[T]

	channel chan lifecycleEvent

	takeAndProcessItem func(getAndRemoveItem func() T)
	finalize           func()
}

func NewScheduler[T any](takeAndProcessItem func(getAndRemoveItem func() T), finalize func(), bufSize ...int) Scheduler[T] {
	s := &scheduler[T]{
		acceptNewItems:     &atomic.Bool{},
		queue:              NewThreadSafeQueue[T](),
		takeAndProcessItem: takeAndProcessItem,
		finalize:           finalize,
	}
	if len(bufSize) == 0 {
		s.channel = make(chan lifecycleEvent, DefaultBufferSize)
	} else {
		s.channel = make(chan lifecycleEvent, bufSize[0])
	}
	s.acceptNewItems.Store(true)

	go s.start()

	return s
}

func (s *scheduler[T]) start() {
	for lcEvent := range s.channel {
		switch lcEvent {
		case Shutdown:
			return
		case ItemAdded:
			s.takeAndProcessItem(s.getAndRemoveItem)
		}
	}
}

func (s *scheduler[T]) Schedule(item T) error {
	if s.acceptNewItems.Load() {
		s.queue.Push(item)
		s.channel <- ItemAdded
		return nil
	} else {
		return ErrDoesntAcceptTasks
	}
}

func (s *scheduler[T]) Shutdown(gracefulPeriod time.Duration, pollingDelay ...time.Duration) error {
	s.acceptNewItems.Store(false)

	defer func() {
		go func() { // notify channel in parallel since channel buffer can be full and current goroutine will block in that case
			s.channel <- Shutdown
		}()

		s.finalize()
	}()

	var delay time.Duration
	if len(pollingDelay) == 0 {
		delay = 100 * time.Millisecond
	} else {
		delay = pollingDelay[0]
	}

	deadline := time.Now().Add(gracefulPeriod)
	for time.Now().Before(deadline) {
		if s.queue.Size() == 0 {
			return nil
		}
		time.Sleep(delay)
	}
	if s.queue.Size() == 0 {
		return nil
	}
	return ErrGracefulShutdown
}

func (s *scheduler[T]) getAndRemoveItem() T {
	item, err := s.queue.Pop()
	if err != nil {
		panic(err) // TODO: log here
	}
	return item
}
