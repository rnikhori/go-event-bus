package eventbus

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
