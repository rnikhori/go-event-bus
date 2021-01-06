package eventbus

import "testing"

func TestBasicPublishSubscribe(t *testing.T) {
	bus := New()
	var got Event
	bus.Subscribe("topic", func(e Event) {
		got = e
	})

	bus.Publish("topic", Event{Name: "hello", Payload: 42})

	if got.Name != "hello" || got.Payload != 42 {
		t.Fatalf("handler did not receive published event, got %+v", got)
	}
}

func TestMultipleSubscribersSameTopic(t *testing.T) {
	bus := New()
	var calls []string

	bus.Subscribe("topic", func(e Event) { calls = append(calls, "first:"+e.Name) })
	bus.Subscribe("topic", func(e Event) { calls = append(calls, "second:"+e.Name) })

	bus.Publish("topic", Event{Name: "ping"})

	if len(calls) != 2 {
		t.Fatalf("expected 2 handler calls, got %d: %v", len(calls), calls)
	}
	if calls[0] != "first:ping" || calls[1] != "second:ping" {
		t.Fatalf("unexpected call order/content: %v", calls)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	bus := New()
	count := 0

	id := bus.Subscribe("topic", func(e Event) { count++ })
	bus.Publish("topic", Event{Name: "one"})

	bus.Unsubscribe("topic", id)
	bus.Publish("topic", Event{Name: "two"})

	if count != 1 {
		t.Fatalf("expected handler to be called exactly once before unsubscribe, got %d", count)
	}
}
