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

Run the demo:

```sh
go run .
```

## Tests

```sh
go test ./...
```
