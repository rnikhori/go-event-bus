package eventbus_test

import (
	"fmt"

	"github.com/rnikhori/go-event-bus/eventbus"
)

func ExampleBus_Subscribe() {
	bus := eventbus.New()

	id := bus.Subscribe("orders", func(e eventbus.Event) {
		fmt.Println("received:", e.Name, e.Payload)
	})

	bus.Publish("orders", eventbus.Event{Name: "created", Payload: "order-123"})

	bus.Unsubscribe("orders", id)
	bus.Publish("orders", eventbus.Event{Name: "ignored", Payload: "order-123"})

	// Output:
	// received: created order-123
}

func ExampleBus_Subscribe_wildcard() {
	bus := eventbus.New()

	bus.Subscribe("orders.*", func(e eventbus.Event) {
		fmt.Println("order event:", e.Name)
	})

	bus.Publish("orders.created", eventbus.Event{Name: "created"})
	bus.Publish("orders.shipped", eventbus.Event{Name: "shipped"})
	bus.Publish("users.created", eventbus.Event{Name: "created"})

	// Output:
	// order event: created
	// order event: shipped
}

type exampleOrderPlaced struct {
	ID string
}

func ExampleTypedBus() {
	bus := eventbus.New()
	orders := eventbus.NewTyped[exampleOrderPlaced](bus)

	orders.Subscribe("orders.placed", func(name string, payload exampleOrderPlaced) {
		fmt.Println("order:", payload.ID)
	})

	orders.Publish("orders.placed", exampleOrderPlaced{ID: "order-123"})

	// Output:
	// order: order-123
}
