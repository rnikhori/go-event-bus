package eventbus

// TypedHandler receives a strongly typed payload instead of the raw
// interface{} stored on Event.
type TypedHandler[T any] func(name string, payload T)

// TypedBus wraps a Bus to provide a type-safe publish/subscribe API for a
// single payload type T, so callers don't need to type-assert
// Event.Payload at every call site. Non-matching payload types delivered
// to the underlying topic (e.g. from another TypedBus sharing the same
// Bus) are silently ignored by TypedBus handlers.
type TypedBus[T any] struct {
	bus *Bus
}

// NewTyped wraps an existing Bus in a TypedBus for payload type T.
// Multiple TypedBus wrappers, including for different payload types, can
// share the same underlying Bus.
func NewTyped[T any](bus *Bus) *TypedBus[T] {
	return &TypedBus[T]{bus: bus}
}

// Subscribe registers handler for topic, delivering only events whose
// payload is of type T.
func (tb *TypedBus[T]) Subscribe(topic string, handler TypedHandler[T]) uint64 {
	return tb.bus.Subscribe(topic, tb.adapt(handler))
}

func (tb *TypedBus[T]) adapt(handler TypedHandler[T]) Handler {
	return func(e Event) {
		payload, ok := e.Payload.(T)
		if !ok {
			return
		}
		handler(e.Name, payload)
	}
}

// Publish wraps payload in an Event and publishes it synchronously to
// topic.
func (tb *TypedBus[T]) Publish(topic string, payload T) {
	tb.bus.Publish(topic, Event{Name: topic, Payload: payload})
}

// Unsubscribe removes the subscription with the given id from topic.
func (tb *TypedBus[T]) Unsubscribe(topic string, id uint64) {
	tb.bus.Unsubscribe(topic, id)
}

// SubscribeWithPriority registers handler for topic with an explicit
// priority; see Bus.SubscribeWithPriority.
func (tb *TypedBus[T]) SubscribeWithPriority(topic string, priority int, handler TypedHandler[T]) uint64 {
	return tb.bus.SubscribeWithPriority(topic, priority, tb.adapt(handler))
}

// SubscribeWithReplay registers handler for topic and immediately replays
// up to n recently published events of type T for that topic; see
// Bus.SubscribeWithReplay.
func (tb *TypedBus[T]) SubscribeWithReplay(topic string, n int, handler TypedHandler[T]) uint64 {
	return tb.bus.SubscribeWithReplay(topic, n, tb.adapt(handler))
}

// PublishAsync wraps payload in an Event and publishes it to topic using
// Bus.PublishAsync, returning the resulting Handle.
func (tb *TypedBus[T]) PublishAsync(topic string, payload T) *Handle {
	return tb.bus.PublishAsync(topic, Event{Name: topic, Payload: payload})
}
