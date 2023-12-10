package queue

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type runnableMock struct {
	num int
	wg  *sync.WaitGroup
}

func (m runnableMock) run() {
	printWithTimestamp("start runnable#%d", m.num)
	time.Sleep(SingleEventProcessingTime)
	printWithTimestamp("finish runnable#%d", m.num)
	m.wg.Done()
}

func TestScheduler_Buffered(t *testing.T) {
	testScheduler := NewWorkerPool(1)

	wg := &sync.WaitGroup{}
	wg.Add(10)

	for i := 1; i <= 10; i++ {
		printWithTimestamp("scheduling runnable#%d", i)
		startTime := time.Now()
		assert.Nil(t, testScheduler.Schedule(runnableMock{num: i, wg: wg}.run))
		endTime := time.Now()
		printWithTimestamp("scheduled runnable#%d", i)
		assert.True(t, startTime.Add(1*time.Second).After(endTime))
	}

	wg.Wait()

	assert.Nil(t, testScheduler.Shutdown(2*time.Second, 100*time.Millisecond))
}

func TestScheduler_Unbuffered(t *testing.T) {
	testScheduler := NewWorkerPool(1, 0)

	wg := &sync.WaitGroup{}
	wg.Add(10)

	scheduleBlockedAtLeastOnce := false
	for i := 1; i <= 10; i++ {
		printWithTimestamp("scheduling runnable#%d", i)
		startTime := time.Now()
		assert.Nil(t, testScheduler.Schedule(runnableMock{num: i, wg: wg}.run))
		endTime := time.Now()
		printWithTimestamp("scheduled runnable#%d", i)
		scheduleBlockedAtLeastOnce = scheduleBlockedAtLeastOnce || startTime.Add(999*time.Millisecond).Before(endTime)
	}
	assert.True(t, scheduleBlockedAtLeastOnce)

	wg.Wait()

	assert.Nil(t, testScheduler.Shutdown(2*time.Second, 100*time.Millisecond))
}
