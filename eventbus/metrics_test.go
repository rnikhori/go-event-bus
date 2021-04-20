package eventbus

import "testing"

func TestStatsCountsAcrossTopicsAndHandlers(t *testing.T) {
	bus := New()

	bus.Subscribe("orders", func(e Event) {})
	bus.Subscribe("orders", func(e Event) {})
	bus.Subscribe("users", func(e Event) {})

	bus.Publish("orders", Event{Name: "created"})
	bus.Publish("orders", Event{Name: "shipped"})
	bus.Publish("users", Event{Name: "created"})

	stats := bus.Stats()

	if stats.Published != 3 {
		t.Fatalf("expected 3 total published, got %d", stats.Published)
	}
	if stats.Delivered != 5 {
		t.Fatalf("expected 5 total delivered (2+2+1), got %d", stats.Delivered)
	}

	orders := stats.PerTopic["orders"]
	if orders.Published != 2 || orders.Delivered != 4 {
		t.Fatalf("expected orders stats {2 4}, got %+v", orders)
	}

	users := stats.PerTopic["users"]
	if users.Published != 1 || users.Delivered != 1 {
		t.Fatalf("expected users stats {1 1}, got %+v", users)
	}
}

func TestStatsCountsDeliveryEvenWhenHandlerPanics(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(e Event) { panic("boom") })

	bus.Publish("topic", Event{Name: "ping"})

	stats := bus.Stats()
	if stats.Delivered != 1 {
		t.Fatalf("expected delivered count to include panicking invocation, got %d", stats.Delivered)
	}
}
