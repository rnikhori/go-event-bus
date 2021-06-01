package eventbus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishAsyncDeliversToAllHandlers(t *testing.T) {
	bus := New()
	var count int64
	var wg sync.WaitGroup
	wg.Add(3)

	for i := 0; i < 3; i++ {
		bus.Subscribe("topic", func(e Event) {
			atomic.AddInt64(&count, 1)
			wg.Done()
		})
	}

	h := bus.PublishAsync("topic", Event{Name: "ping"})
	h.Wait()
	wg.Wait()

	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("expected 3 handler invocations, got %d", got)
	}
}

func TestHandleWaitBlocksUntilHandlersFinish(t *testing.T) {
	bus := New()
	var finished int32

	bus.Subscribe("topic", func(e Event) {
		time.Sleep(20 * time.Millisecond)
		atomic.StoreInt32(&finished, 1)
	})

	h := bus.PublishAsync("topic", Event{Name: "ping"})
	h.Wait()

	if atomic.LoadInt32(&finished) != 1 {
		t.Fatal("expected Wait to block until handler finished")
	}
}

func TestHandleDoneChannelClosesAfterCompletion(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(e Event) {})

	h := bus.PublishAsync("topic", Event{Name: "ping"})

	select {
	case <-h.Done():
	case <-time.After(time.Second):
		t.Fatal("expected Done channel to close within timeout")
	}
}

func TestPublishAsyncUpdatesStats(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(e Event) {})
	bus.Subscribe("topic", func(e Event) {})

	bus.PublishAsync("topic", Event{Name: "ping"}).Wait()

	stats := bus.Stats()
	if stats.Published != 1 {
		t.Fatalf("expected published count 1, got %d", stats.Published)
	}
	if stats.Delivered != 2 {
		t.Fatalf("expected delivered count 2, got %d", stats.Delivered)
	}
}
