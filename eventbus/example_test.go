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
