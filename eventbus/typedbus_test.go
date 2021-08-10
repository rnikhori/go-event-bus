package eventbus

import "testing"

type OrderCreated struct {
	ID string
}

func TestTypedBusDeliversTypedPayload(t *testing.T) {
	bus := New()
	typed := NewTyped[OrderCreated](bus)

	var got OrderCreated
	typed.Subscribe("orders.created", func(name string, payload OrderCreated) {
		got = payload
	})

	typed.Publish("orders.created", OrderCreated{ID: "order-1"})

	if got.ID != "order-1" {
		t.Fatalf("expected typed handler to receive payload, got %+v", got)
	}
}

func TestTypedBusIgnoresMismatchedPayloadType(t *testing.T) {
	bus := New()
	typed := NewTyped[OrderCreated](bus)

	called := false
	typed.Subscribe("topic", func(name string, payload OrderCreated) {
		called = true
	})

	bus.Publish("topic", Event{Name: "topic", Payload: "not an OrderCreated"})

	if called {
		t.Fatal("expected typed handler to ignore a payload of the wrong type")
	}
}

func TestTypedBusSubscribeWithReplay(t *testing.T) {
	bus := New()
	typed := NewTyped[OrderCreated](bus)

	typed.Publish("orders.created", OrderCreated{ID: "order-1"})
	typed.Publish("orders.created", OrderCreated{ID: "order-2"})

	var replayed []string
	typed.SubscribeWithReplay("orders.created", 5, func(name string, payload OrderCreated) {
		replayed = append(replayed, payload.ID)
	})

	if len(replayed) != 2 || replayed[0] != "order-1" || replayed[1] != "order-2" {
		t.Fatalf("expected replay of both orders, got %v", replayed)
	}
}

func TestTypedBusPublishAsync(t *testing.T) {
	bus := New()
	typed := NewTyped[OrderCreated](bus)

	var got OrderCreated
	typed.Subscribe("orders.created", func(name string, payload OrderCreated) {
		got = payload
	})

	typed.PublishAsync("orders.created", OrderCreated{ID: "order-1"}).Wait()

	if got.ID != "order-1" {
		t.Fatalf("expected async typed handler to receive payload, got %+v", got)
	}
}
