package main

import (
	"fmt"

	"github.com/rnikhori/go-event-bus/eventbus"
)

func main() {
	bus := eventbus.New()

	basicDemo(bus)
	wildcardDemo(bus)
}

func basicDemo(bus *eventbus.Bus) {
	fmt.Println("--- basic pub/sub ---")

	bus.Subscribe("orders", func(e eventbus.Event) {
		fmt.Printf("[audit] order event %q: %v\n", e.Name, e.Payload)
	})

	bus.Subscribe("orders", func(e eventbus.Event) {
		fmt.Printf("[billing] charging for order: %v\n", e.Payload)
	})

	noisyID := bus.Subscribe("system", func(e eventbus.Event) {
		fmt.Printf("[metrics] system event %q: %v\n", e.Name, e.Payload)
	})

	bus.Publish("orders", eventbus.Event{Name: "created", Payload: "order-123"})
	bus.Publish("system", eventbus.Event{Name: "heartbeat", Payload: 1})

	bus.Unsubscribe("system", noisyID)
	fmt.Println("unsubscribed metrics handler from system topic")

	bus.Publish("system", eventbus.Event{Name: "heartbeat", Payload: 2})
	bus.Publish("orders", eventbus.Event{Name: "shipped", Payload: "order-123"})
}

func wildcardDemo(bus *eventbus.Bus) {
	fmt.Println("\n--- wildcard subscriptions ---")

	bus.Subscribe("orders.*", func(e eventbus.Event) {
		fmt.Printf("[orders.*] %q: %v\n", e.Name, e.Payload)
	})

	bus.Publish("orders.created", eventbus.Event{Name: "created", Payload: "order-456"})
	bus.Publish("orders.shipped", eventbus.Event{Name: "shipped", Payload: "order-456"})
	bus.Publish("users.created", eventbus.Event{Name: "created", Payload: "user-1"})
}
