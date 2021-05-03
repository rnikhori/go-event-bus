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

// PanicHandler is invoked, outside of any internal lock, whenever a
// subscribed Handler panics during dispatch. It receives the topic the
// event was published on, the event itself, and the recovered value.
type PanicHandler func(topic string, event Event, recovered interface{})

type subscription struct {
	id       uint64
	priority int
	handler  Handler
}

// Bus keys subscriptions by the pattern string passed to Subscribe. A
// pattern with no "*" segments behaves as a literal topic name.
type Bus struct {
	mu      sync.Mutex
	subs    map[string][]subscription
	next    uint64
	onPanic PanicHandler

	published uint64
	delivered uint64
	perTopic  map[string]*topicStats

	history    map[string][]Event
	historyCap map[string]int
}

type topicStats struct {
	published uint64
	delivered uint64
}

func New() *Bus {
	return &Bus{
		subs:       make(map[string][]subscription),
		perTopic:   make(map[string]*topicStats),
		history:    make(map[string][]Event),
		historyCap: make(map[string]int),
	}
}

// perTopicLocked returns the stats bucket for topic, creating it if
// necessary. b.mu must be held by the caller.
func (b *Bus) perTopicLocked(topic string) *topicStats {
	ts, ok := b.perTopic[topic]
	if !ok {
		ts = &topicStats{}
		b.perTopic[topic] = ts
	}
	return ts
}

// Subscribe registers handler for topic (which may be a wildcard pattern,
// see MatchTopic) and returns an id that can be passed to Unsubscribe to
// stop delivery to this handler.
func (b *Bus) Subscribe(topic string, handler func(event Event)) uint64 {
	return b.SubscribeWithPriority(topic, 0, handler)
}

// SubscribeWithPriority registers handler for topic like Subscribe, but
// with an explicit priority. During synchronous Publish, handlers with a
// higher priority run before handlers with a lower one; handlers that
// share a priority run in subscription order. The default priority used by
// Subscribe is 0.
func (b *Bus) SubscribeWithPriority(topic string, priority int, handler Handler) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subscribeLocked(topic, priority, handler)
}

func (b *Bus) subscribeLocked(topic string, priority int, handler Handler) uint64 {
	b.next++
	id := b.next
	b.subs[topic] = append(b.subs[topic], subscription{id: id, priority: priority, handler: handler})
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
// topic, ordered first by subscription id (the order handlers were
// registered in) and then stably by priority, highest first.
func (b *Bus) matchingSubs(topic string) []subscription {
	var result []subscription
	for pattern, subs := range b.subs {
		if MatchTopic(pattern, topic) {
			result = append(result, subs...)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	sort.SliceStable(result, func(i, j int) bool { return result[i].priority > result[j].priority })
	return result
}

// Publish synchronously delivers event to every handler currently
// subscribed to a pattern matching topic, in subscription order. A handler
// that panics does not stop delivery to the remaining handlers; see
// dispatchOne.
func (b *Bus) Publish(topic string, event Event) {
	b.mu.Lock()
	subs := b.matchingSubs(topic)
	b.published++
	b.perTopicLocked(topic).published++
	b.recordHistoryLocked(topic, event)
	b.mu.Unlock()

	for _, s := range subs {
		b.dispatchOne(s, topic, event)
	}
}

// OnPanic registers a callback invoked whenever a handler panics during
// dispatch. Only one callback can be registered at a time; a later call
// replaces the previous one. Pass nil to stop observing panics.
func (b *Bus) OnPanic(handler PanicHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onPanic = handler
}

// dispatchOne invokes a single handler, recovering from any panic so that
// one misbehaving handler cannot block delivery to the rest of the
// subscribers or crash the publishing goroutine.
func (b *Bus) dispatchOne(s subscription, topic string, event Event) {
	b.mu.Lock()
	onPanic := b.onPanic
	b.delivered++
	b.perTopicLocked(topic).delivered++
	b.mu.Unlock()

	defer func() {
		if r := recover(); r != nil && onPanic != nil {
			onPanic(topic, event, r)
		}
	}()
	s.handler(event)
}
