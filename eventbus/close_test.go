package eventbus

import (
	"errors"
	"testing"
)

func TestCloseRejectsNewSubscriptions(t *testing.T) {
	bus := New()
	if err := bus.Close(); err != nil {
		t.Fatalf("unexpected error closing bus: %v", err)
	}

	if id := bus.Subscribe("topic", func(e Event) {}); id != 0 {
		t.Fatalf("expected Subscribe on closed bus to return id 0, got %d", id)
	}
	if id := bus.SubscribeWithPriority("topic", 5, func(e Event) {}); id != 0 {
		t.Fatalf("expected SubscribeWithPriority on closed bus to return id 0, got %d", id)
	}
	if id := bus.SubscribeWithReplay("topic", 5, func(e Event) {}); id != 0 {
		t.Fatalf("expected SubscribeWithReplay on closed bus to return id 0, got %d", id)
	}
}

func TestCloseStopsPublishDelivery(t *testing.T) {
	bus := New()
	var called bool
	bus.Subscribe("topic", func(e Event) { called = true })

	if err := bus.Close(); err != nil {
		t.Fatalf("unexpected error closing bus: %v", err)
	}
	bus.Publish("topic", Event{Name: "ping"})

	if called {
		t.Fatal("expected no delivery to subscribers registered before Close")
	}
}

func TestClosePublishAsyncReturnsCompletedHandle(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(e Event) {})

	if err := bus.Close(); err != nil {
		t.Fatalf("unexpected error closing bus: %v", err)
	}

	h := bus.PublishAsync("topic", Event{Name: "ping"})
	select {
	case <-h.Done():
	default:
		t.Fatal("expected Handle for closed bus to be immediately done")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	bus := New()
	if err := bus.Close(); err != nil {
		t.Fatalf("unexpected error on first Close: %v", err)
	}
	if err := bus.Close(); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("expected ErrAlreadyClosed on second Close, got %v", err)
	}
	if !bus.Closed() {
		t.Fatal("expected Closed() to report true after Close")
	}
}
