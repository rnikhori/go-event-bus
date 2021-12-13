package eventbus

import (
	"reflect"
	"sort"
	"testing"
)

func TestBasicPublishSubscribe(t *testing.T) {
	bus := New()
	var got Event
	bus.Subscribe("topic", func(e Event) {
		got = e
	})

	bus.Publish("topic", Event{Name: "hello", Payload: 42})

	if got.Name != "hello" || got.Payload != 42 {
		t.Fatalf("handler did not receive published event, got %+v", got)
	}
}

func TestMultipleSubscribersSameTopic(t *testing.T) {
	bus := New()
	var calls []string

	bus.Subscribe("topic", func(e Event) { calls = append(calls, "first:"+e.Name) })
	bus.Subscribe("topic", func(e Event) { calls = append(calls, "second:"+e.Name) })

	bus.Publish("topic", Event{Name: "ping"})

	if len(calls) != 2 {
		t.Fatalf("expected 2 handler calls, got %d: %v", len(calls), calls)
	}
	if calls[0] != "first:ping" || calls[1] != "second:ping" {
		t.Fatalf("unexpected call order/content: %v", calls)
	}
}

func TestPublishOnlyReachesExactTopicSubscribers(t *testing.T) {
	bus := New()
	var ordersCalls, usersCalls int

	bus.Subscribe("orders", func(e Event) { ordersCalls++ })
	bus.Subscribe("users", func(e Event) { usersCalls++ })

	bus.Publish("orders", Event{Name: "created"})

	if ordersCalls != 1 {
		t.Fatalf("expected orders handler to be called once, got %d", ordersCalls)
	}
	if usersCalls != 0 {
		t.Fatalf("expected users handler to not be called, got %d", usersCalls)
	}
}

func TestWildcardSubscriptionReceivesMatchingTopics(t *testing.T) {
	bus := New()
	var received []string

	bus.Subscribe("orders.*", func(e Event) { received = append(received, e.Name) })

	bus.Publish("orders.created", Event{Name: "created"})
	bus.Publish("orders.shipped", Event{Name: "shipped"})
	bus.Publish("users.created", Event{Name: "created"})

	if len(received) != 2 || received[0] != "created" || received[1] != "shipped" {
		t.Fatalf("expected wildcard subscriber to receive [created shipped], got %v", received)
	}
}

func TestWildcardAndExactSubscribersBothFire(t *testing.T) {
	bus := New()
	var calls []string

	bus.Subscribe("orders.created", func(e Event) { calls = append(calls, "exact") })
	bus.Subscribe("orders.*", func(e Event) { calls = append(calls, "wildcard") })

	bus.Publish("orders.created", Event{Name: "created"})

	if len(calls) != 2 {
		t.Fatalf("expected both exact and wildcard subscribers to fire, got %v", calls)
	}
}

func TestPanicInHandlerDoesNotStopOtherHandlers(t *testing.T) {
	bus := New()
	var secondCalled bool

	bus.Subscribe("topic", func(e Event) { panic("boom") })
	bus.Subscribe("topic", func(e Event) { secondCalled = true })

	bus.Publish("topic", Event{Name: "ping"})

	if !secondCalled {
		t.Fatal("expected second handler to run despite first handler panicking")
	}
}

func TestOnPanicReceivesRecoveredValue(t *testing.T) {
	bus := New()
	var gotTopic string
	var gotRecovered interface{}

	bus.OnPanic(func(topic string, event Event, recovered interface{}) {
		gotTopic = topic
		gotRecovered = recovered
	})
	bus.Subscribe("topic", func(e Event) { panic("boom") })

	bus.Publish("topic", Event{Name: "ping"})

	if gotTopic != "topic" {
		t.Fatalf("expected OnPanic to receive topic %q, got %q", "topic", gotTopic)
	}
	if gotRecovered != "boom" {
		t.Fatalf("expected OnPanic to receive recovered value %q, got %v", "boom", gotRecovered)
	}
}

func TestPriorityOrdersSynchronousDispatch(t *testing.T) {
	bus := New()
	var order []string

	bus.SubscribeWithPriority("topic", 0, func(e Event) { order = append(order, "low") })
	bus.SubscribeWithPriority("topic", 10, func(e Event) { order = append(order, "high") })
	bus.SubscribeWithPriority("topic", 5, func(e Event) { order = append(order, "mid") })

	bus.Publish("topic", Event{Name: "ping"})

	want := []string{"high", "mid", "low"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("expected dispatch order %v, got %v", want, order)
	}
}

func TestEqualPriorityKeepsSubscriptionOrder(t *testing.T) {
	bus := New()
	var order []string

	bus.SubscribeWithPriority("topic", 1, func(e Event) { order = append(order, "first") })
	bus.SubscribeWithPriority("topic", 1, func(e Event) { order = append(order, "second") })
	bus.SubscribeWithPriority("topic", 1, func(e Event) { order = append(order, "third") })

	bus.Publish("topic", Event{Name: "ping"})

	want := []string{"first", "second", "third"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("expected dispatch order %v, got %v", want, order)
	}
}

func TestTopicsListsOnlyPatternsWithActiveSubscriptions(t *testing.T) {
	bus := New()

	if got := bus.Topics(); len(got) != 0 {
		t.Fatalf("expected no topics on a fresh bus, got %v", got)
	}

	id := bus.Subscribe("orders", func(e Event) {})
	bus.Subscribe("orders.*", func(e Event) {})

	got := bus.Topics()
	sort.Strings(got)
	want := []string{"orders", "orders.*"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected topics %v, got %v", want, got)
	}

	bus.Unsubscribe("orders", id)

	got = bus.Topics()
	want = []string{"orders.*"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected topics %v after unsubscribe, got %v", want, got)
	}
}

func TestSubscriberCountTracksRegistrationsAndUnsubscribe(t *testing.T) {
	bus := New()

	if got := bus.SubscriberCount("topic"); got != 0 {
		t.Fatalf("expected 0 subscribers before any Subscribe, got %d", got)
	}

	id1 := bus.Subscribe("topic", func(e Event) {})
	bus.Subscribe("topic", func(e Event) {})

	if got := bus.SubscriberCount("topic"); got != 2 {
		t.Fatalf("expected 2 subscribers, got %d", got)
	}

	bus.Unsubscribe("topic", id1)

	if got := bus.SubscriberCount("topic"); got != 1 {
		t.Fatalf("expected 1 subscriber after unsubscribe, got %d", got)
	}
}

func TestUnsubscribeAllRemovesEveryHandlerOnPattern(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(e Event) {})
	bus.Subscribe("topic", func(e Event) {})
	bus.Subscribe("other", func(e Event) {})

	removed := bus.UnsubscribeAll("topic")
	if removed != 2 {
		t.Fatalf("expected UnsubscribeAll to report 2 removed handlers, got %d", removed)
	}
	if got := bus.SubscriberCount("topic"); got != 0 {
		t.Fatalf("expected 0 subscribers on topic after UnsubscribeAll, got %d", got)
	}
	if got := bus.SubscriberCount("other"); got != 1 {
		t.Fatalf("expected other topic to be unaffected, got %d subscribers", got)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	bus := New()
	count := 0

	id := bus.Subscribe("topic", func(e Event) { count++ })
	bus.Publish("topic", Event{Name: "one"})

	bus.Unsubscribe("topic", id)
	bus.Publish("topic", Event{Name: "two"})

	if count != 1 {
		t.Fatalf("expected handler to be called exactly once before unsubscribe, got %d", count)
	}
}
