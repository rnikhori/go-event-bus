package eventbus

import "strings"

// MatchTopic reports whether topic matches pattern. A pattern is a sequence
// of dot-separated segments where a literal "*" segment matches exactly one
// segment of topic at the same position. For example, "orders.*" matches
// "orders.created" and "orders.shipped" but not "orders" or
// "orders.created.v2".
func MatchTopic(pattern, topic string) bool {
	if pattern == topic {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}

	patternSegments := strings.Split(pattern, ".")
	topicSegments := strings.Split(topic, ".")
	if len(patternSegments) != len(topicSegments) {
		return false
	}

	for i, seg := range patternSegments {
		if seg == "*" {
			continue
		}
		if seg != topicSegments[i] {
			return false
		}
	}
	return true
}
