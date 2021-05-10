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

## Panic isolation

If a handler panics during `Publish`, the bus recovers and continues
delivering the event to the remaining subscribers. Register `OnPanic` to
observe (e.g. log) recovered panics instead of silently discarding them:

```go
bus.OnPanic(func(topic string, e eventbus.Event, recovered interface{}) {
	log.Printf("handler for %q panicked: %v", topic, recovered)
})
```

## Handler priority

`SubscribeWithPriority` lets a handler run before or after others on the
same topic during synchronous `Publish`. Higher priority runs first; ties
fall back to registration order:

```go
bus.SubscribeWithPriority("orders.created", 10, func(e eventbus.Event) {
	fmt.Println("validate first")
})
bus.SubscribeWithPriority("orders.created", 0, func(e eventbus.Event) {
	fmt.Println("then notify")
})
```

## Metrics

`Stats()` returns a snapshot of how many events have been published and how
many handler invocations have happened, both bus-wide and per topic:

```go
stats := bus.Stats()
fmt.Println("total published:", stats.Published)
fmt.Println("total delivered:", stats.Delivered)
fmt.Println("orders delivered:", stats.PerTopic["orders"].Delivered)
```

A handler invocation counts toward `Delivered` even if the handler panics.

## Event history and replay

The bus retains a capped buffer of recently published events per topic.
`SubscribeWithReplay` subscribes a handler and immediately replays up to
`n` of the most recent past events for that topic before returning, then
continues delivering live events as normal:

```go
bus.Publish("orders", eventbus.Event{Name: "created"})
bus.Publish("orders", eventbus.Event{Name: "paid"})

// A subscriber that joins late still sees recent history first.
bus.SubscribeWithReplay("orders", 10, func(e eventbus.Event) {
	fmt.Println("catching up on:", e.Name)
})
```

`SubscribeWithReplay` only supports exact topic names, not wildcard
patterns.

Run the demo:

```sh
go run .
```

## Tests

```sh
go test ./...
```
