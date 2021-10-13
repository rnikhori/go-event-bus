package eventbus

import "testing"

func BenchmarkPublishSingleHandler(b *testing.B) {
	bus := New()
	bus.Subscribe("topic", func(e Event) {})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("topic", Event{Name: "ping"})
	}
}

func BenchmarkPublishManyHandlers(b *testing.B) {
	bus := New()
	for i := 0; i < 20; i++ {
		bus.Subscribe("topic", func(e Event) {})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("topic", Event{Name: "ping"})
	}
}

func BenchmarkPublishWildcardMatch(b *testing.B) {
	bus := New()
	bus.Subscribe("orders.*", func(e Event) {})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("orders.created", Event{Name: "created"})
	}
}

func BenchmarkPublishAsync(b *testing.B) {
	bus := New()
	for i := 0; i < 20; i++ {
		bus.Subscribe("topic", func(e Event) {})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.PublishAsync("topic", Event{Name: "ping"}).Wait()
	}
}
