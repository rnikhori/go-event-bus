# go-event-bus

A minimal, thread-safe in-memory pub/sub event bus for Go, built on the standard library only.

## Usage

```go
package main

import (
	"fmt"

	"github.com/rnikhori/go-event-bus/eventbus"
)

func main() {
	bus := eventbus.New()

	id := bus.Subscribe("orders", func(e eventbus.Event) {
		fmt.Println("received:", e.Name, e.Payload)
	})

	bus.Publish("orders", eventbus.Event{Name: "created", Payload: "order-123"})

	bus.Unsubscribe("orders", id)
}
```

## Wildcard subscriptions

Subscribe to a dot-separated pattern instead of a literal topic name. A `*`
segment matches exactly one segment of the published topic:

```go
bus.Subscribe("orders.*", func(e eventbus.Event) {
	fmt.Println("order event:", e.Name, e.Payload)
})

bus.Publish("orders.created", eventbus.Event{Name: "created"}) // matches
bus.Publish("orders.shipped", eventbus.Event{Name: "shipped"}) // matches
bus.Publish("orders", eventbus.Event{Name: "created"})         // does not match
```

Exact-topic subscribers and wildcard subscribers can both be registered for
overlapping topics; a published event reaches every matching subscription.

Run the demo:

```sh
go run .
```

## Tests

```sh
go test ./...
```
