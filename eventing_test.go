package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const SingleEventProcessingTime = 1 * time.Second

type TestEvent struct {
	Num int
}

type testSrv struct {
	succeededEventsNum *atomic.Int32
	retriesNum         *atomic.Int32

	t            *testing.T
	wg           *sync.WaitGroup
	falloutQueue EventQueue[*TestEvent]
}

func (srv *testSrv) Process(event *TestEvent) {
	printWithTimestamp("Start processing event#%d", event.Num)

	time.Sleep(SingleEventProcessingTime)

	if event.Num <= 10 {
		panic("panic in event#" + fmt.Sprintf("%d", event.Num))
	} else if event.Num > 90 {
		srv.scheduleFailedEvent(event)
		return
	}

	printWithTimestamp("Finished processing event#%d", event.Num)
	srv.succeededEventsNum.Add(1)
	srv.wg.Done()
}

func (srv *testSrv) ProcessFallout(event *TestEvent) {
	printWithTimestamp("Processing fallout event#%d", event.Num)

	srv.retriesNum.Add(1)

	time.Sleep(SingleEventProcessingTime)

	printWithTimestamp("Fallout event#%d processed successfully", event.Num)
	srv.succeededEventsNum.Add(1)
	srv.wg.Done()
}

func (srv *testSrv) scheduleFailedEvent(event *TestEvent) {
	printWithTimestamp("Rescheduling event#%d", event.Num)
	assert.Nil(srv.t, srv.falloutQueue.Schedule(event))
	printWithTimestamp("Failed event#%d rescheduled", event.Num)
}

func TestEventQueue(t *testing.T) {
	mainQueue := NewEventQueue[*TestEvent](EventQueueOpts{MaxParallelProcessors: 32})
	falloutQueue := NewEventQueue[*TestEvent](EventQueueOpts{MaxParallelProcessors: 1, Delay: 2 * time.Second})

	wg := &sync.WaitGroup{}
	wg.Add(90)

	srv := testSrv{
		succeededEventsNum: &atomic.Int32{},
		retriesNum:         &atomic.Int32{},
		t:                  t,
		wg:                 wg,
		falloutQueue:       falloutQueue,
	}

	mainQueue.Subscribe(srv.Process)
	falloutQueue.Subscribe(srv.ProcessFallout)

	for i := 1; i <= 100; i++ {
		go scheduleEvent(t, mainQueue, i)
	}

	wg.Wait()

	assert.Equal(t, int32(90), srv.succeededEventsNum.Load())
	assert.Equal(t, int32(10), srv.retriesNum.Load())

	assert.Nil(t, mainQueue.Shutdown(2*time.Second, 100*time.Millisecond))
	assert.Nil(t, falloutQueue.Shutdown(2*time.Second, 100*time.Millisecond))
}

func scheduleEvent(t *testing.T, mainQueue EventQueue[*TestEvent], eventNumber int) {
	assert.Nil(t, mainQueue.Schedule(&TestEvent{eventNumber}))
}

func printWithTimestamp(msg string, args ...any) {
	fmt.Printf("[%s] %s\r\n", time.Now().Format(time.RFC3339), fmt.Sprintf(msg, args...))
}
