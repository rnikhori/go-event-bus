package eventbus

import "sync"

// Handle represents an in-flight PublishAsync call, letting callers wait
// for every dispatched handler to finish.
type Handle struct {
	wg   sync.WaitGroup
	done chan struct{}
}

func newHandle() *Handle {
	return &Handle{done: make(chan struct{})}
}

// Wait blocks until every handler dispatched by the PublishAsync call that
// returned this Handle has finished running, including handlers that
// panicked.
func (h *Handle) Wait() {
	h.wg.Wait()
}

// Done returns a channel that is closed once every handler dispatched by
// the PublishAsync call that returned this Handle has finished running.
// It is suitable for use in a select statement alongside other channels.
func (h *Handle) Done() <-chan struct{} {
	return h.done
}

// PublishAsync behaves like Publish, except each matching handler is
// invoked in its own goroutine and PublishAsync returns immediately. The
// returned Handle can be used to wait for all of the dispatched handlers
// to finish. Since handlers run concurrently, there is no guaranteed
// completion order between them, even when they were registered with
// different priorities.
func (b *Bus) PublishAsync(topic string, event Event) *Handle {
	h := newHandle()

	b.mu.Lock()
	subs := b.matchingSubs(topic)
	b.published++
	b.perTopicLocked(topic).published++
	b.recordHistoryLocked(topic, event)
	b.mu.Unlock()

	h.wg.Add(len(subs))
	for _, s := range subs {
		s := s
		go func() {
			defer h.wg.Done()
			b.dispatchOne(s, topic, event)
		}()
	}

	go func() {
		h.wg.Wait()
		close(h.done)
	}()

	return h
}
