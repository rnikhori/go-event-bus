package eventbus

// TopicStats holds a snapshot of publish/delivery counts for a single
// topic.
type TopicStats struct {
	Published uint64
	Delivered uint64
}

// Stats holds a snapshot of bus-wide publish/delivery counts, along with a
// per-topic breakdown.
type Stats struct {
	Published uint64
	Delivered uint64
	PerTopic  map[string]TopicStats
}

// Stats returns a point-in-time snapshot of the bus's metrics: the total
// number of Publish/PublishAsync calls, the total number of handler
// invocations across all topics, and the same two counts broken down by
// topic.
func (b *Bus) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := Stats{
		Published: b.published,
		Delivered: b.delivered,
		PerTopic:  make(map[string]TopicStats, len(b.perTopic)),
	}
	for topic, ts := range b.perTopic {
		out.PerTopic[topic] = TopicStats{Published: ts.published, Delivered: ts.delivered}
	}
	return out
}
