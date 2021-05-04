package eventbus

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
	replay := make([]Event, len(hist))
	copy(replay, hist)
	id := b.subscribeLocked(topic, 0, handler)
	b.mu.Unlock()

	for _, e := range replay {
		handler(e)
	}
	return id
}

// recordHistoryLocked appends event to topic's history buffer if a replay
// buffer has been requested for topic (via SubscribeWithReplay), trimming
// the buffer to its configured capacity. b.mu must be held by the caller.
func (b *Bus) recordHistoryLocked(topic string, event Event) {
	capacity := b.historyCap[topic]
	if capacity <= 0 {
		return
	}

	buf := append(b.history[topic], event)
	if len(buf) > capacity {
		buf = buf[len(buf)-capacity:]
	}
	b.history[topic] = buf
}
