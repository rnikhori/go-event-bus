package eventbus

// defaultHistoryCapacity is how many recent events are retained per topic
// before any SubscribeWithReplay call has requested a larger capacity.
const defaultHistoryCapacity = 64

// SubscribeWithReplay registers handler for topic (an exact topic, not a
// wildcard pattern) and immediately replays up to n of the most recently
// published events for that topic to handler before returning, so a new
// subscriber can catch up on recent history. n is also used to grow the
// topic's history buffer capacity for future replays if it is larger than
// any previously requested capacity.
func (b *Bus) SubscribeWithReplay(topic string, n int, handler Handler) uint64 {
	b.mu.Lock()
	if n > b.historyCap[topic] {
		b.historyCap[topic] = n
	}
	hist := b.history[topic]
	if n >= 0 && len(hist) > n {
		hist = hist[len(hist)-n:]
	}
	replay := make([]Event, len(hist))
	copy(replay, hist)
	id := b.subscribeLocked(topic, 0, handler)
	b.mu.Unlock()

	for _, e := range replay {
		handler(e)
	}
	return id
}

// recordHistoryLocked appends event to topic's history buffer, trimming it
// to the topic's configured capacity (or defaultHistoryCapacity if none has
// been explicitly requested yet via SubscribeWithReplay). b.mu must be held
// by the caller.
func (b *Bus) recordHistoryLocked(topic string, event Event) {
	capacity := b.historyCap[topic]
	if capacity <= 0 {
		capacity = defaultHistoryCapacity
	}

	buf := append(b.history[topic], event)
	if len(buf) > capacity {
		buf = buf[len(buf)-capacity:]
	}
	b.history[topic] = buf
}
