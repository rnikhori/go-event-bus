// Package eventbus provides a minimal in-memory, thread-safe publish/subscribe bus.
package eventbus

import "sync"

type Event struct {
	Name    string
	Payload interface{}
}

type Handler func(event Event)

type subscription struct {
	id      uint64
	handler Handler
}

type Bus struct {
	mu   sync.RWMutex
	subs map[string][]subscription
	next uint64
}

func New() *Bus {
	return &Bus{subs: make(map[string][]subscription)}
}

// Subscribe registers handler for topic and returns an id that can be
// passed to Unsubscribe to stop delivery to this handler.
func (b *Bus) Subscribe(topic string, handler func(event Event)) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.next++
	id := b.next
	b.subs[topic] = append(b.subs[topic], subscription{id: id, handler: handler})
	return id
}

// Unsubscribe removes the subscription with the given id from topic, if present.
func (b *Bus) Unsubscribe(topic string, id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs := b.subs[topic]
	for i, s := range subs {
		if s.id == id {
			b.subs[topic] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

// Publish synchronously delivers event to every handler currently
// subscribed to topic, in subscription order.
func (b *Bus) Publish(topic string, event Event) {
	b.mu.RLock()
	subs := make([]subscription, len(b.subs[topic]))
	copy(subs, b.subs[topic])
	b.mu.RUnlock()

	for _, s := range subs {
		s.handler(event)
	}
}
