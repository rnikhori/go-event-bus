# go-event-bus

A thread-safe in-memory pub/sub event bus for Go, built on the standard
library only.

## Features

- [Basic pub/sub](#usage) with `Subscribe`, `Publish`, `Unsubscribe`
- [Wildcard topic subscriptions](#wildcard-subscriptions), e.g. `orders.*`
- [Panic isolation](#panic-isolation) during dispatch, with an `OnPanic` hook
- [Handler priority](#handler-priority) for synchronous `Publish`
- [Metrics](#metrics) via `Stats()`, bus-wide and per topic
- [Event history and replay](#event-history-and-replay) for late subscribers
- [Async publish](#async-publish) via `PublishAsync` and `Handle`
- [Clean shutdown](#shutdown) via `Close`
- [Generic typed wrapper](#typed-wrapper), `TypedBus[T]`

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

## Async publish

`PublishAsync` dispatches to every matching handler in its own goroutine
and returns immediately with a `*Handle`. Use `Wait()` to block until every
dispatched handler has finished, or `Done()` for a channel suitable for use
in a `select`:

```go
h := bus.PublishAsync("orders.created", eventbus.Event{Name: "created", Payload: "order-123"})
h.Wait() // or: <-h.Done()
```

Handlers dispatched this way still benefit from panic recovery, but since
they run concurrently there is no guaranteed completion order between
them, even across different priorities.

## Shutdown

`Close` stops the bus from accepting new subscriptions or publishes.
`Subscribe`/`SubscribeWithPriority`/`SubscribeWithReplay` return an id of 0
and `Publish`/`PublishAsync` become no-ops after `Close`:

```go
bus.Close()
bus.Publish("orders", eventbus.Event{Name: "ignored"}) // delivered to nobody
```

`Close` does not wait for handlers already dispatched by an earlier
`PublishAsync` call to finish; call `Wait()` on their `Handle` first if
that matters. `Close` is idempotent: calling it again returns
`ErrAlreadyClosed`.

## Typed wrapper

`TypedBus[T]` wraps a `*Bus` to give a single payload type a type-safe API,
avoiding `interface{}` assertions at call sites:

```go
type OrderCreated struct {
	ID string
}

orders := eventbus.NewTyped[OrderCreated](bus)

orders.Subscribe("orders.created", func(name string, payload OrderCreated) {
	fmt.Println("order:", payload.ID)
})

orders.Publish("orders.created", OrderCreated{ID: "order-123"})
```

A `TypedBus` supports `Subscribe`, `SubscribeWithPriority`,
`SubscribeWithReplay`, `Publish`, `PublishAsync`, and `Unsubscribe`.
Multiple `TypedBus` wrappers, including for different payload types, can
share the same underlying `Bus`; a `TypedBus` handler silently ignores
events whose payload isn't of type `T`.

## Design notes

- **Thread-safety.** Every exported method on `Bus` is safe to call
  concurrently from multiple goroutines. Handlers are always invoked
  outside of the bus's internal lock, so a slow or blocking handler does
  not stall other callers of `Subscribe`/`Publish`/`Stats`/etc.
- **Ordering.** `Publish` delivers to matching handlers in subscription
  order, then stably reordered by priority (highest first). `PublishAsync`
  makes no ordering guarantee between handlers, including handlers at
  different priorities, since they run concurrently.
- **Panics.** A handler that panics during dispatch (sync or async) is
  recovered and still counts toward `Stats().Delivered`; it does not stop
  delivery to the remaining handlers for that event.
- **History.** Each topic retains up to the largest capacity ever
  requested via `SubscribeWithReplay` for that topic (64 events by
  default). History is keyed by the exact topic string published to, not
  by subscription pattern.
- **Close.** `Close` is a point-in-time switch: it rejects new
  subscriptions and publishes from that moment on, but it does not cancel
  or wait for handlers already dispatched by an in-flight `PublishAsync`.

Run the demo:

```sh
go run .
```

## Tests

```sh
go test ./...
```
