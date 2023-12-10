package queue

import (
	"sync"
	"time"
)

type Runnable func()

type WorkerPool struct {
	Scheduler[Runnable]
	mutex *sync.Mutex

	activeWorkers int
	size          int
}

func NewWorkerPool(size int, bufSize ...int) *WorkerPool {
	pool := &WorkerPool{
		mutex:         &sync.Mutex{},
		activeWorkers: 0,
		size:          size,
	}
	pool.Scheduler = NewScheduler[Runnable](pool.processNewItem, NoOp, bufSize...)
	return pool
}

func (p *WorkerPool) processNewItem(getAndRemoveItem func() Runnable) {
	for {
		if p.activeWorkers < p.size { // if there are free workers
			if p.takeWorker() { // take worker with double check
				go p.executeAndReleaseWorker(getAndRemoveItem)
				return
			}
		}
		time.Sleep(100 * time.Millisecond) // in case there was no free worker - sleep and try again
	}
}

func (p *WorkerPool) executeAndReleaseWorker(getAndRemoveItem func() Runnable) {
	defer p.releaseWorker()

	p.executeItemWithRecovery(getAndRemoveItem)
}

func (p *WorkerPool) executeItemWithRecovery(getAndRemoveItem func() Runnable) {
	runnable := getAndRemoveItem()

	defer func() {
		if r := recover(); r != nil {
			//			panic(r)
			// TODO: log here
		}
	}()
	runnable()
}

func (p *WorkerPool) takeWorker() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.activeWorkers < p.size {
		p.activeWorkers += 1
		return true
	}
	return false
}

func (p *WorkerPool) releaseWorker() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.activeWorkers -= 1
}

func NoOp() {}
