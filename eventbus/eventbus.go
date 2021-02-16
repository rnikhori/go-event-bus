// Package eventbus provides a minimal in-memory, thread-safe publish/subscribe bus.
package eventbus

import (
	"sort"
	"sync"
)

type Event struct {
	Name    string
	Payload interface{}
}

type Handler func(event Event)

type subscription struct {
	id      uint64
	handler Handler
}

// Bus keys subscriptions by the pattern string passed to Subscribe. A
// pattern with no "*" segments behaves as a literal topic name.
type Bus struct {
	mu   sync.Mutex
	subs map[string][]subscription
	next uint64
}

func New() *Bus {
	return &Bus{subs: make(map[string][]subscription)}
}

// Subscribe registers handler for topic (which may be a wildcard pattern,
// see MatchTopic) and returns an id that can be passed to Unsubscribe to
// stop delivery to this handler.
func (b *Bus) Subscribe(topic string, handler func(event Event)) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subscribeLocked(topic, handler)
}

func (b *Bus) subscribeLocked(topic string, handler Handler) uint64 {
	b.next++
	id := b.next
	b.subs[topic] = append(b.subs[topic], subscription{id: id, handler: handler})
	return id
}

// Unsubscribe removes the subscription with the given id, registered under
// the exact pattern string originally passed to Subscribe, if present.
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

// matchingSubs returns every subscription whose registered pattern matches
// topic, ordered by subscription id (i.e. the order handlers were
// registered in).
func (b *Bus) matchingSubs(topic string) []subscription {
	var result []subscription
	for pattern, subs := range b.subs {
		if MatchTopic(pattern, topic) {
			result = append(result, subs...)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result
}

// Publish synchronously delivers event to every handler currently
// subscribed to a pattern matching topic, in subscription order. A handler
// that panics does not stop delivery to the remaining handlers; see
// dispatchOne.
func (b *Bus) Publish(topic string, event Event) {
	b.mu.Lock()
	subs := b.matchingSubs(topic)
	b.mu.Unlock()

	for _, s := range subs {
		b.dispatchOne(s, topic, event)
	}
}

// dispatchOne invokes a single handler, recovering from any panic so that
// one misbehaving handler cannot block delivery to the rest of the
// subscribers or crash the publishing goroutine.
func (b *Bus) dispatchOne(s subscription, topic string, event Event) {
	defer func() {
		recover()
	}()
	s.handler(event)
}
