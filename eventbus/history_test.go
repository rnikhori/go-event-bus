package eventbus

import (
	"reflect"
	"testing"
)

func TestSubscribeWithReplayReceivesRecentHistory(t *testing.T) {
	bus := New()
	bus.Subscribe("orders", func(e Event) {})

	bus.Publish("orders", Event{Name: "created"})
	bus.Publish("orders", Event{Name: "paid"})
	bus.Publish("orders", Event{Name: "shipped"})

	var replayed []string
	bus.SubscribeWithReplay("orders", 2, func(e Event) {
		replayed = append(replayed, e.Name)
	})

	want := []string{"paid", "shipped"}
	if !reflect.DeepEqual(replayed, want) {
		t.Fatalf("expected replay %v, got %v", want, replayed)
	}
}

func TestHistoryBufferCappedAtRequestedSize(t *testing.T) {
	bus := New()
	bus.SubscribeWithReplay("orders", 2, func(e Event) {})

	bus.Publish("orders", Event{Name: "one"})
	bus.Publish("orders", Event{Name: "two"})
	bus.Publish("orders", Event{Name: "three"})

	var replayed []string
	bus.SubscribeWithReplay("orders", 2, func(e Event) {
		replayed = append(replayed, e.Name)
	})

	want := []string{"two", "three"}
	if !reflect.DeepEqual(replayed, want) {
		t.Fatalf("expected capped history %v, got %v", want, replayed)
	}
}

func TestSubscribeWithReplayThenReceivesLiveEvents(t *testing.T) {
	bus := New()
	bus.Publish("orders", Event{Name: "created"})

	var received []string
	bus.SubscribeWithReplay("orders", 5, func(e Event) {
		received = append(received, e.Name)
	})
	bus.Publish("orders", Event{Name: "shipped"})

	want := []string{"created", "shipped"}
	if !reflect.DeepEqual(received, want) {
		t.Fatalf("expected replay followed by live delivery %v, got %v", want, received)
	}
}
