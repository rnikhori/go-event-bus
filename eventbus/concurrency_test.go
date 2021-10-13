package eventbus

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentSubscribePublishUnsubscribe stresses Subscribe, Publish,
// PublishAsync, and Unsubscribe from many goroutines at once. It does not
// assert on exact counts (delivery counts under concurrent Unsubscribe are
// inherently racy by design), but it must complete without the race
// detector flagging a data race and without panicking.
func TestConcurrentSubscribePublishUnsubscribe(t *testing.T) {
	bus := New()
	var delivered int64

	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				topic := fmt.Sprintf("topic.%d", g%5)
				id := bus.Subscribe(topic, func(e Event) {
					atomic.AddInt64(&delivered, 1)
				})
				bus.Unsubscribe(topic, id)
			}
		}(g)

		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				topic := fmt.Sprintf("topic.%d", g%5)
				bus.Publish(topic, Event{Name: "ping"})
			}
		}(g)

		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				topic := fmt.Sprintf("topic.%d", g%5)
				bus.PublishAsync(topic, Event{Name: "ping"}).Wait()
			}
		}(g)
	}

	wg.Wait()

	stats := bus.Stats()
	if stats.Published == 0 {
		t.Fatal("expected at least some published events to be recorded")
	}
}
